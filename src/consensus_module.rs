use std::fmt::{Debug, Formatter};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use rand::Rng;
use rand_core::RngCore;
use tonic::Request;

use crate::server::Server;
use crate::CMState::{Candidate, Dead, Follower, Leader};
use crate::{raft, server, Args, CMState, Reply};
use tokio::sync::RwLock;

static DEBUGCM: bool = true;

pub struct ConsensusModule {
    pub id: usize,
    pub peer_ids: Vec<usize>,
    pub current_term: i32,
    pub voted_for: i32,
    pub server: Option<Arc<RwLock<Server>>>,
    pub log: Vec<raft::LogEntry>,
    pub state: CMState,
    pub election_reset_event: Option<SystemTime>,
}

impl ConsensusModule {
    pub fn new(id: usize, peer_ids: Vec<usize>) -> Arc<RwLock<ConsensusModule>> {
        let cm: Arc<RwLock<ConsensusModule>> = Arc::new(RwLock::new(ConsensusModule {
            id,
            peer_ids,
            current_term: i32::default(),
            voted_for: -1,
            server: None,
            log: Vec::new(),
            state: CMState::Follower,
            election_reset_event: None,
        }));

        let (tx, rx): (
            Sender<Arc<RwLock<ConsensusModule>>>,
            Receiver<Arc<RwLock<ConsensusModule>>>,
        ) = channel();

        tx.send(Arc::clone(&cm)).unwrap();
        tokio::spawn(async move {
            let arc = rx.recv().unwrap();
            let mut cm_thread = arc.write().await;
            cm_thread.election_reset_event = Some(SystemTime::now());
            std::mem::drop(cm_thread);
            Self::run_election_timer(arc.clone()).await;
        });

        cm
    }

    pub async fn report(arc_cm: Arc<RwLock<ConsensusModule>>) -> (i32, i32, bool) {
        let cm = arc_cm.read().await;
        let res = (
            cm.id.clone() as i32,
            cm.current_term.clone(),
            cm.state == Leader,
        );
        std::mem::drop(cm);
        res
    }

    pub fn stop(&mut self) {
        self.state = CMState::Dead;
        ConsensusModule::dlog(self.id, "becomes Dead");
    }

    pub fn dlog(cm_id: usize, str: &str) {
        if DEBUGCM {
            println!("{}", format!("[{}] {}", cm_id, str));
        }
    }

    pub async fn request_vote(
        arc_cm: Arc<RwLock<ConsensusModule>>,
        args: server::raft::RequestVoteArgs,
    ) -> Option<server::raft::RequestVoteReply> {
        let mut cm = arc_cm.read().await;
        if cm.state == Dead {
            return None;
        }
        ConsensusModule::dlog(
            cm.id,
            format!(
                "RequestVote: {:?} [current_term = {:?}, voted_for = {:?}]",
                args, cm.current_term, cm.voted_for
            )
            .as_str(),
        );
        if args.term > cm.current_term {
            ConsensusModule::dlog(cm.id, "... term out of date in RequestVote");
            std::mem::drop(cm);
            ConsensusModule::become_follower(Arc::clone(&arc_cm), args.term).await;
        }
        cm = arc_cm.read().await;
        let mut reply = server::raft::RequestVoteReply {
            term: 0,
            vote_granted: false,
        };

        if cm.current_term == args.term && (cm.voted_for == -1 || cm.voted_for == args.candidate_id)
        {
            reply.vote_granted = true;
            std::mem::drop(cm);
            let mut wl = arc_cm.write().await;
            wl.voted_for = args.candidate_id;
            wl.election_reset_event = Some(SystemTime::now());
            std::mem::drop(wl);
        }
        cm = arc_cm.read().await;
        reply.term = cm.current_term;
        ConsensusModule::dlog(cm.id, format!("... RequestVote reply {:?}", reply).as_str());
        std::mem::drop(cm);
        Some(reply)
    }

    pub async fn append_entries(
        arc_cm: Arc<RwLock<ConsensusModule>>,
        args: server::raft::AppendEntriesArgs,
    ) -> Option<server::raft::AppendEntriesReply> {
        let mut cm_read = arc_cm.read().await;
        if cm_read.state == Dead {
            return None;
        }
        ConsensusModule::dlog(cm_read.id, format!("AppendEntries {:?}", args).as_str());
        if args.term > cm_read.current_term {
            ConsensusModule::dlog(cm_read.id, "... term out of date in AppendEntries");
            std::mem::drop(cm_read);
            ConsensusModule::become_follower(Arc::clone(&arc_cm), args.term).await;
            cm_read = arc_cm.read().await;
        }
        std::mem::drop(cm_read);

        let mut reply = server::raft::AppendEntriesReply {
            term: 0,
            success: false,
        };
        let mut cm_write = arc_cm.write().await;
        if args.term == cm_write.current_term {
            if cm_write.state != Follower {
                std::mem::drop(cm_write);
                ConsensusModule::become_follower(Arc::clone(&arc_cm), args.term).await;
                cm_write = arc_cm.write().await;
            }
            cm_write.election_reset_event = Some(SystemTime::now());
            reply.success = true;
        }
        reply.term = cm_write.current_term;
        ConsensusModule::dlog(cm_write.id, format!("AppendEntries reply {:?}", reply).as_str());
        std::mem::drop(cm_write);
        Some(reply)
    }

    fn election_timeout(mut rngcore: Box<dyn RngCore>) -> std::time::Duration {
        let range_num = rngcore.gen_range(0..3);
        return if std::env::var("FORCE_REELECTION").is_ok() && range_num == 0 {
            Duration::from_millis(150)
        } else {
            Duration::from_millis(150 + rngcore.gen_range(0..150))
        };
    }

    async fn run_election_timer(arc_cm: Arc<RwLock<ConsensusModule>>) {
        let timeout_duration = ConsensusModule::election_timeout(Box::new(rand::thread_rng()));
        let cm_read = arc_cm.read().await;

        let term_started = cm_read.current_term;
        ConsensusModule::dlog(
            cm_read.id,
            format!(
                "election timer started {:?}, term {:?}",
                timeout_duration, term_started
            )
            .as_str(),
        );
        std::mem::drop(cm_read);

        loop {
            sleep(Duration::from_millis(10));
            let cm = arc_cm.read().await;
            if cm.state != Candidate && cm.state != Follower {
                ConsensusModule::dlog(
                    cm.id,
                    format!("in election timer state {:?}, bailing out ", cm.state).as_str(),
                );
                std::mem::drop(cm);
                return;
            }

            if term_started != cm.current_term {
                ConsensusModule::dlog(
                    cm.id,
                    format!(
                        "in election timer term changed from {:?} to {:?}, bailing out ",
                        term_started, cm.current_term
                    )
                    .as_str(),
                );
                std::mem::drop(cm);
                return;
            }

            let elapsed_time = cm.election_reset_event.unwrap().elapsed().unwrap();
            if elapsed_time >= timeout_duration {
                std::mem::drop(cm);
                Self::start_election_sync(Arc::clone(&arc_cm));
                return;
            }
            std::mem::drop(cm);
        }
    }

    fn start_election_sync(cm: Arc<RwLock<ConsensusModule>>) {
        tokio::task::spawn(async move {
            Self::start_election(cm).await;
        });
    }

    async fn start_election(arc_cm: Arc<RwLock<ConsensusModule>>) {
        let arc_cm_election_timer_thread = Arc::clone(&arc_cm);
        let mut cm = arc_cm.write().await;
        cm.state = Candidate;
        cm.current_term += 1;
        let saved_current_term = cm.current_term;
        cm.election_reset_event = Some(SystemTime::now());
        cm.voted_for = cm.id as i32;
        let vec = cm.peer_ids.clone();
        ConsensusModule::dlog(
            cm.id,
            format!(
                "becomes candidate (currentTerm = {:?},candidate_id :{:?} ); log = {:?}",
                saved_current_term, cm.id, cm.log
            )
            .as_str(),
        );
        std::mem::drop(cm);
        let votes_received = Arc::new(tokio::sync::Mutex::new(1));

        for p in vec {
            let p_clone = p.clone();
            let votes_received_clone = Arc::clone(&votes_received);
            let (tx, rx): (
                Sender<Arc<RwLock<ConsensusModule>>>,
                Receiver<Arc<RwLock<ConsensusModule>>>,
            ) = channel();
            tx.send(Arc::clone(&arc_cm)).unwrap();
            tokio::spawn(async move {
                let cm = rx.recv().expect("error occurred while receiving");
                let cm_thread = cm.read().await;

                let args = server::raft::RequestVoteArgs {
                    term: saved_current_term,
                    candidate_id: cm_thread.id as i32,
                    last_log_index: 0,
                    last_log_term: 0,
                };
                ConsensusModule::dlog(
                    cm_thread.id,
                    format!("sending request vote to {:?}: {:?}", p_clone, args).as_str(),
                );
                let server = Arc::clone(cm_thread.server.as_ref().unwrap());
                std::mem::drop(cm_thread);
                let arc_cm_req_vote_thread = Arc::clone(&cm);
                let result = Server::call(server, p_clone, Args::RequestVote(Request::new(args)))
                    .await
                    .unwrap();
                match result {
                    Some(Reply::RequestVote(r)) => {
                        let read_lock = arc_cm_req_vote_thread.read().await;
                        let inner_val = r.into_inner();
                        ConsensusModule::dlog(
                            read_lock.id,
                            format!(
                                "received RequestVoteReply term {:?} votes_granted {:?}",
                                inner_val.term, inner_val.vote_granted
                            )
                            .as_str(),
                        );
                        if read_lock.state != Candidate {
                            ConsensusModule::dlog(
                                read_lock.id,
                                format!("waiting for reply state {:?}", read_lock.state).as_str(),
                            );
                            return;
                        }
                        std::mem::drop(read_lock);

                        if inner_val.term > saved_current_term {
                            let read_lock = arc_cm_req_vote_thread.read().await;
                            ConsensusModule::dlog(
                                read_lock.id,
                                "term out of date in RequestVoteReply",
                            );
                            std::mem::drop(read_lock);
                            ConsensusModule::become_follower(
                                Arc::clone(&arc_cm_req_vote_thread),
                                inner_val.term,
                            )
                            .await;
                            return;
                        } else if inner_val.term == saved_current_term {
                            let mut read_lock = arc_cm_req_vote_thread.read().await;
                            if inner_val.vote_granted {
                                let mut votes_recv_lock = votes_received_clone.lock().await;
                                *votes_recv_lock += 1;
                                let total_votes_recv_so_far = *votes_recv_lock;
                                std::mem::drop(votes_recv_lock);
                                if total_votes_recv_so_far * 2
                                    > (read_lock.peer_ids.len() + 1) as i32
                                {
                                    ConsensusModule::dlog(
                                        read_lock.id,
                                        format!(
                                            "wins election with {:?} votes server id {:?}",
                                            total_votes_recv_so_far, read_lock.id
                                        )
                                        .as_str(),
                                    );
                                    std::mem::drop(read_lock);
                                    let mut write_lock = arc_cm_req_vote_thread.write().await;
                                    write_lock.state = Leader;
                                    std::mem::drop(write_lock);
                                    read_lock = arc_cm_req_vote_thread.read().await;
                                    ConsensusModule::dlog(
                                        read_lock.id,
                                        format!(
                                            "becomes leader; term {:?}, log {:?}",
                                            read_lock.current_term, read_lock.log
                                        )
                                        .as_str(),
                                    );
                                    std::mem::drop(read_lock);
                                    ConsensusModule::start_leader(Arc::clone(
                                        &arc_cm_req_vote_thread,
                                    ))
                                    .await;
                                    return;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            })
            .await
            .unwrap();
        }

        tokio::spawn(async move {
            ConsensusModule::run_election_timer(arc_cm_election_timer_thread).await;
        });
    }

    async fn become_follower(arc_cm: Arc<RwLock<ConsensusModule>>, term: i32) {
        let mut cm = arc_cm.write().await;
        ConsensusModule::dlog(
            cm.id,
            format!("becomes follower with Term {:?}, log {:?}", term, cm.log).as_str(),
        );

        cm.state = CMState::Follower;
        cm.current_term = term;
        cm.voted_for = -1;
        cm.election_reset_event = Some(SystemTime::now());

        std::mem::drop(cm);

        let arc_cm_clone = Arc::clone(&arc_cm);
        tokio::spawn(async move { Self::run_election_timer(arc_cm_clone).await });
    }

    async fn start_leader(cm: Arc<RwLock<ConsensusModule>>) {
        let cm_thread = Arc::clone(&cm);
        tokio::spawn(async move {
            loop {
                ConsensusModule::leader_send_heartbeats(Arc::clone(&cm_thread)).await;
                sleep(Duration::from_millis(50));
                let cm_mutex = cm_thread.read().await;
                if cm_mutex.state != Leader {
                    return;
                }
                std::mem::drop(cm_mutex);
            }
        });
    }

    async fn leader_send_heartbeats(cm: Arc<RwLock<ConsensusModule>>) {
        let cm_lock = cm.read().await;
        if cm_lock.state != Leader {
            return;
        }
        let saved_current_term = cm_lock.current_term;
        let peer_ids = cm_lock.peer_ids.clone();
        std::mem::drop(cm_lock);
        for peer_id in peer_ids {
            let arc_clone_thread = Arc::clone(&cm);

            tokio::spawn(async move {
                let cm_lock = arc_clone_thread.read().await;
                let args = server::raft::AppendEntriesArgs {
                    term: saved_current_term,
                    leader_id: cm_lock.id as i32,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                };
                let cm_id = cm_lock.id;
                let server = Arc::clone(cm_lock.server.as_ref().unwrap());
                std::mem::drop(cm_lock);

                ConsensusModule::dlog(
                    cm_id,
                    format!(
                        " sending AppendEntries to {:?}: ni={:?}, args={:?} ",
                        peer_id, 0, args
                    )
                    .as_str(),
                );
                let reply =
                    Server::call(server, peer_id, Args::AppendEntries(Request::new(args))).await;
                match reply {
                    Ok(Some(Reply::AppendEntries(r))) => {
                        let inner_val = r.into_inner();
                        if inner_val.term > saved_current_term {
                            ConsensusModule::dlog(cm_id, "term out of date in heartbeat reply");
                            tokio::spawn(async move {
                                ConsensusModule::become_follower(
                                    Arc::clone(&arc_clone_thread),
                                    inner_val.term,
                                )
                                .await;
                            });
                            return;
                        }
                    }
                    Err(e) => ConsensusModule::dlog(cm_id, format!("{:?}", e).as_str()),
                    _ => {}
                }
            })
            .await
            .unwrap();
        }
    }
}

impl Debug for ConsensusModule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            format!(
                " [ id {:?} \n peer_ids {:?} \n current_term {:?} \n \
        voted_for {:?} \n log {:?} \n state {:?} \n election_reset_event {:?}]",
                self.id,
                self.peer_ids,
                self.current_term,
                self.voted_for,
                self.log,
                self.state,
                self.election_reset_event
            )
        )
    }
}
