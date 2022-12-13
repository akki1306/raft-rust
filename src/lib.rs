pub mod consensus_module;
pub mod server;

use consensus_module::ConsensusModule;
use rand::rngs::StdRng;
use rand::Rng;
use rand_core::SeedableRng;
use server::raft::raft_server::Raft;
use std::error::Error;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub mod raft {
    tonic::include_proto!("raft");
}

pub struct RaftRpc {
    pub rpc_proxy: RpcProxy,
}

impl RaftRpc {
    pub fn new(_: SocketAddr, rpc_proxy: RpcProxy) -> RaftRpc {
        RaftRpc { rpc_proxy }
    }
}

#[tonic::async_trait]
impl Raft for RaftRpc {
    async fn request_vote(
        &self,
        request: Request<server::raft::RequestVoteArgs>,
    ) -> Result<Response<server::raft::RequestVoteReply>, Status> {
        Ok(Response::new(
            self.rpc_proxy
                .request_vote(request.into_inner())
                .await
                .unwrap()
                .unwrap(),
        ))
    }

    async fn append_entries(
        &self,
        request: Request<server::raft::AppendEntriesArgs>,
    ) -> Result<Response<server::raft::AppendEntriesReply>, Status> {
        Ok(Response::new(
            self.rpc_proxy
                .append_entries(request.into_inner())
                .await
                .unwrap()
                .unwrap(),
        ))
    }
}

#[derive(Debug)]
pub enum Args {
    RequestVote(tonic::Request<server::raft::RequestVoteArgs>),
    AppendEntries(tonic::Request<server::raft::AppendEntriesArgs>),
}

#[derive(Debug)]
pub enum Reply {
    RequestVote(tonic::Response<server::raft::RequestVoteReply>),
    AppendEntries(tonic::Response<server::raft::AppendEntriesReply>),
}

#[derive(PartialEq, Debug)]
pub enum CMState {
    Follower,
    Candidate,
    Leader,
    Dead,
}

pub struct RpcProxy {
    pub cm: Arc<RwLock<ConsensusModule>>,
}

impl RpcProxy {
    async fn request_vote(
        &self,
        args: server::raft::RequestVoteArgs,
    ) -> Result<Option<server::raft::RequestVoteReply>, Box<dyn std::error::Error>> {
        let mut rng = StdRng::seed_from_u64(100);
        let range_num = rng.gen_range(0..10);
        if std::env::var("RAFT_UNRELIABLE_RPC").is_ok() {
            if range_num == 9 {
                return Err(Box::<dyn Error>::from("Rpc failed"));
            } else if range_num == 8 {
                sleep(std::time::Duration::from_millis(75));
            }
        } else {
            sleep(Duration::from_millis(1 + rng.gen_range(0..5)));
        }
        Ok(ConsensusModule::request_vote(Arc::clone(&self.cm), args).await)
    }

    async fn append_entries(
        &self,
        args: server::raft::AppendEntriesArgs,
    ) -> Result<Option<server::raft::AppendEntriesReply>, Box<dyn std::error::Error>> {
        let mut rng = StdRng::seed_from_u64(100);
        let range_num = rng.gen_range(0..10);
        if std::env::var("RAFT_UNRELIABLE_RPC").is_ok() {
            if range_num == 9 {
                return Err(Box::<dyn Error>::from("Rpc failed"));
            } else if range_num == 8 {
                sleep(std::time::Duration::from_millis(75));
            }
        } else {
            sleep(Duration::from_millis(1 + rng.gen_range(0..5)));
        }

        Ok(ConsensusModule::append_entries(Arc::clone(&self.cm), args).await)
    }
}
