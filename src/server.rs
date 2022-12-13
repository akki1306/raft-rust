use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::{env, net::SocketAddr};

use futures::FutureExt;
use tokio::sync::{oneshot, RwLock};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use raft::raft_client::RaftClient;

use crate::server::tests::Harness;
use crate::{ConsensusModule, RaftRpc, Reply, RpcProxy};

pub mod raft {
    tonic::include_proto!("raft");
}

pub struct Server {
    pub server_id: usize,
    pub cm: Arc<RwLock<ConsensusModule>>,
    pub listener: Option<SocketAddr>,
    pub peer_clients: HashMap<usize, RaftClient<tonic::transport::Channel>>,
    pub quit: Option<oneshot::Sender<i32>>,
}

impl Server {
    pub async fn new(server_id: usize, peer_ids: Vec<usize>) -> Arc<RwLock<Server>> {
        let arc = ConsensusModule::new(server_id, peer_ids.clone());
        let s = Arc::new(RwLock::new(Server {
            server_id,
            cm: Arc::clone(&arc),
            listener: None,
            peer_clients: HashMap::new(),
            quit: None,
        }));
        let mut write_lock = arc.write().await;
        write_lock.server = Some(Arc::clone(&s));
        std::mem::drop(write_lock);
        s
    }

    pub async fn serve(addr: SocketAddr, server: Arc<RwLock<Server>>) -> Result<(), String> {
        tokio::spawn(async move {
            let mut x = server.write().await;
            let cm = Arc::clone(&x.cm);
            let rpc_proxy = RpcProxy {
                cm: Arc::clone(&cm),
            };
            let mut url = "127.0.0.1:".to_owned();
            url.push_str(&addr.port().to_string());
            let sock_addr: SocketAddr = url.parse().unwrap();
            x.listener = Some(sock_addr);
            let (tx, rx): (oneshot::Sender<i32>, oneshot::Receiver<i32>) = oneshot::channel();
            x.quit = Some(tx);
            std::mem::drop(x);
            let raft_rpc = RaftRpc { rpc_proxy };
            tonic::transport::Server::builder()
                .add_service(raft::raft_server::RaftServer::new(raft_rpc))
                .serve_with_shutdown(sock_addr, rx.map(drop))
                .await
                .unwrap();
        });
        Ok(())
    }

    pub async fn get_listen_address(
        server: Arc<RwLock<Server>>,
    ) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        let s = server.read().await;
        return if s.listener.is_some() {
            Ok(s.listener.unwrap())
        } else {
            Err(Box::<dyn Error>::from(
                "error occurred while fetching listen address",
            ))
        };
    }

    pub async fn connect_to_peer(
        server: Arc<RwLock<Server>>,
        peer_id: usize,
        addr: SocketAddr,
    ) -> Result<(), String> {
        let mut url = "http://127.0.0.1:".to_owned();
        url.push_str(&addr.port().to_string());
        //log::debug!("url is {}", url);
        let client = RaftClient::connect(url).await;

        match client {
            Ok(c) => {
                let mut s = server.write().await;
                s.peer_clients.entry(peer_id).or_insert(c.clone());
                std::mem::drop(s);
            }
            Err(e) => {
                Harness::tlog(format!("Error occurred while creating client {:?}", e).as_str())
            }
        }

        Ok(())
    }

    pub async fn disconnect_peer(server: Arc<RwLock<Server>>, peer_id: usize) {
        let mut s = server.write().await;
        let v = s.peer_clients.remove_entry(&peer_id);
        if v.is_some() {
            std::mem::drop(v);
        }
        std::mem::drop(s);
    }

    pub async fn disconnect_all(server: Arc<RwLock<Server>>) {
        //Harness::tlog("trying to acquire write lock in disconnect_all");
        let mut s = server.write().await;
        //Harness::tlog("acquired lock in disconnect_all");
        s.peer_clients.clear();
        // Harness::tlog(format!("peer clients length is {}", s.peer_clients.len()).as_str());
        std::mem::drop(s);
    }

    pub async fn shutdown(server: Arc<RwLock<Server>>) {
        let mut s = server.write().await;
        s.listener = None;

        if s.quit.is_some() {
            s.quit.take().unwrap().send(1).unwrap();
        }
        std::mem::drop(s);
        s = server.write().await;
        let mut cm_write_lock = s.cm.write().await;
        cm_write_lock.stop();
        std::mem::drop(cm_write_lock);
        std::mem::drop(s);
    }

    pub async fn call(
        server: Arc<RwLock<Server>>,
        peer_id: usize,
        args: crate::Args,
    ) -> Result<Option<Reply>, Box<dyn std::error::Error>> {
        let server_lock = server.write().await;
        let mut map = server_lock.peer_clients.clone();
        let mut option = map.get_mut(&peer_id).take();
        std::mem::drop(server_lock);
        if option.is_some() {
            let channel: &mut &mut RaftClient<tonic::transport::Channel> = option.as_mut().unwrap();
            let reply: Reply = match args {
                crate::Args::RequestVote(r) => Reply::RequestVote(channel.request_vote(r).await?),
                crate::Args::AppendEntries(r) => {
                    Reply::AppendEntries(channel.append_entries(r).await?)
                }
            };
            return Ok(Some(reply));
        }
        Ok(None)
    }
}

/// Initializes an OpenTelmetry tracing subscriber with a Jaeger backend.
pub fn init_tracing(service_name: &str) -> anyhow::Result<()> {
    env::set_var("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "12");

    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        .with_max_packet_size(2usize.pow(13))
        .install_batch(opentelemetry::runtime::Tokio)?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(())
}

//#[cfg(test)]
pub mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use tokio::sync::{Mutex, RwLock};

    use crate::server::Server;
    use crate::ConsensusModule;

    pub struct Harness {
        // cluster is a list of all the raft servers participating in a cluster.
        pub cluster: Vec<Arc<RwLock<Server>>>,
        // connected has a bool per server in cluster, specifying whether this server
        // is currently connected to peers (if false, it's partitioned and no messages
        // will pass to or from it).
        connected: Vec<bool>,

        n: usize,
    }

    impl Harness {
        pub async fn new(n: usize) -> Arc<Mutex<Harness>> {
            let mut h = Harness {
                cluster: vec![],
                connected: vec![],
                n,
            };
            for _ in 0..n {
                h.connected.push(false);
            }

            let arc_harness = Arc::new(Mutex::new(h));
            for i in 0..n {
                let mut peer_ids = vec![];
                for j in 0..n {
                    if i != j {
                        peer_ids.push(j);
                    }
                }
                let server = Server::new(i, peer_ids).await;
                let sock_addr =
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50000u16 + i as u16);
                let mut mutex = arc_harness.lock().await;
                mutex.cluster.push(Arc::clone(&server));
                let mut server_lock = server.write().await;
                server_lock.listener = Some(sock_addr);
                std::mem::drop(server_lock);
                Server::serve(sock_addr, Arc::clone(&server)).await.unwrap();
            }

            let (tx, rx): (Sender<Arc<Mutex<Harness>>>, Receiver<Arc<Mutex<Harness>>>) = channel();
            for _ in 0..n {
                tx.send(Arc::clone(&arc_harness)).unwrap();
            }

            for i in 0..n {
                let arc_harness_clone = rx.recv().unwrap();
                let mut arc_mutex = arc_harness_clone.lock().await;
                for j in 0..n {
                    if i != j {
                        let x = arc_mutex.cluster.get(j).unwrap();
                        let sock_addr = Server::get_listen_address(Arc::clone(x)).await.unwrap();
                        let s = arc_mutex.cluster.get(i).unwrap();
                        Server::connect_to_peer(Arc::clone(s), j, sock_addr)
                            .await
                            .unwrap();
                    }
                }
                arc_mutex.connected.insert(i, true);
            }
            arc_harness
        }

        pub async fn shutdown(&mut self) {
            for i in 0..self.n {
                let arc_clone = Arc::clone(self.cluster.get(i).unwrap());
                Server::disconnect_all(arc_clone).await;
                self.connected[i] = false;
            }

            for i in 0..self.n {
                let x = self.cluster.get(i).unwrap();
                let mut write_lock = x.write().await;
                write_lock.quit.take().unwrap().send(1).unwrap();
                std::mem::drop(write_lock);
                Server::shutdown(Arc::clone(x)).await;
            }
        }

        pub async fn disconnect_peer(&mut self, id: usize) {
            Harness::tlog(format!("Disconnect {:?}", id).as_str());
            Server::disconnect_all(Arc::clone(self.cluster.get(id).unwrap())).await;
            for i in 0..self.n {
                if i != id {
                    Server::disconnect_peer(Arc::clone(self.cluster.get(i).unwrap()), id).await;
                }
            }
            self.connected[id] = false;
        }

        pub async fn reconnect_peer(
            &mut self,
            id: usize,
        ) -> Result<(), Box<dyn std::error::Error>> {
            Harness::tlog(format!("Reconnect {:?}", id).as_str());
            for i in 0..self.n {
                if i != id {
                    let mut result =
                        Server::get_listen_address(Arc::clone(self.cluster.get(id).unwrap()))
                            .await
                            .unwrap();
                    Server::connect_to_peer(Arc::clone(self.cluster.get(i).unwrap()), id, result)
                        .await
                        .unwrap();
                    result = Server::get_listen_address(Arc::clone(self.cluster.get(i).unwrap()))
                        .await
                        .unwrap();
                    Server::connect_to_peer(Arc::clone(self.cluster.get(id).unwrap()), i, result)
                        .await
                        .unwrap();
                }
                self.connected[id] = true;
            }
            Ok(())
        }

        pub async fn check_single_leader(&self) -> (i32, i32) {
            for i in 0..3 {
                let mut leader_id: i32 = -1;
                let mut leader_term = -1;
                for j in 0..self.n {
                    if self.connected[j] {
                        let mut server_guard = self.cluster.get(j).unwrap().read().await;
                        let cm = Arc::clone(&server_guard.cm);
                        std::mem::drop(server_guard);
                        let (_, term, is_leader) = ConsensusModule::report(cm).await;
                        server_guard = self.cluster.get(j).unwrap().read().await;
                        if is_leader {
                            if leader_id < 0 {
                                leader_id = j as i32;
                                leader_term = term;
                            } else {
                                Harness::tlog(
                                    format!(
                                        "both {:?} and {:?} think they're leaders ",
                                        leader_id, i
                                    )
                                    .as_str(),
                                );
                            }
                        }
                        std::mem::drop(server_guard);
                    }
                }
                if leader_id >= 0 {
                    return (leader_id, leader_term);
                }
                sleep(Duration::from_millis(150));
            }
            (-1, -1)
        }

        pub async fn check_no_leader(&mut self) {
            for i in 0..self.n {
                if self.connected[i] {
                    let read_lock = self.cluster.get(i).unwrap().read().await;
                    let cm = Arc::clone(&read_lock.cm);
                    std::mem::drop(read_lock);
                    let (_, _, is_leader) = ConsensusModule::report(cm).await;
                    if is_leader {
                        eprintln!("server {} leader, want none ", i);
                    }
                }
            }
        }

        pub fn tlog(str: &str) {
            println!("{}", format!("[TEST] {}", str));
        }
    }
}
