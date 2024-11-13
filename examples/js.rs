//! Test interoperability with the nodejs version
//!
//! This will create a bootstrap node and another node that represents stateful
//! nodes in the dht.
//! By running
//!     `cargo run --example local-bs-interop | node js/announce-lookup.test.js`
//! the address of the bootstrap node gets piped to
//! the nodejs program which creates an ephemeral node to announce and
//! afterwards unannouce a topic and a port.
use std::{net::Ipv4Addr, time::Duration};

use futures::StreamExt;
use hyperswarm_dht::{rpc::RpcDht, DhtConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ip: Ipv4Addr = "127.0.0.1".parse().unwrap();
    let port: u16 = 10001;
    let address = [(ip, port)];
    let conf = DhtConfig::default().set_bootstrap_nodes(&address);
    //dbg!(&conf);

    dbg!();
    let mut node = RpcDht::with_config(conf).await?;
    dbg!();
    /*
    while let Some(x) = node.next().await {
        dbg!(&x);
    }
    */
    //dbg!(&node);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        dbg!();
        for kv in node.kbuckets.iter() {
            dbg!(kv.node);
            dbg!(kv.status);
        }
        dbg!();
        if let Some(x) = node.next().await {
            dbg!(&x);
        }
    }
}
