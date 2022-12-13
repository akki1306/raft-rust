# RAFT-Rust

This  is an implementation of RAFT algorithm using Rust language. Purpose of writing this 
implementation is to thorougly understand internal working of RAFT algorithm from implementation
perspective. As a Rustacean by heart, I also got a chance to work on the libraries from 
Rust ecosystem like tokio, tarpc, serde, futures and clap. This implementation is thoroughly tested
and is fully functional although not recommended for production use. 

Test cases are written in main.rs, you can follow debug logs to check interaction happening between different
nodes spawned as tokio threads and communicating with each other using hyperium tonic RPC. 