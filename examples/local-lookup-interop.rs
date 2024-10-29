//! Test interoperability with the nodejs version
//!
//! This will create 2 nodes, one announcing a port and the other looking it up,
//! connected to a JS bootstrap server to test interop
//! By running
//!     `node js/reverse-announce-lookup.test.js | cargo run --example local-lookup-interop`
//! the address of the bootstrap node gets piped to
//! the rust program which creates an ephemeral node to announce and
//! afterwards unannouce a topic and a port.
use futures::StreamExt;
use hyperswarm_dht::{DhtConfig, HyperDht, IdBytes, QueryOpts};
use tokio::{
    io::{stdin, AsyncBufReadExt, BufReader},
    task,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut bootstrap = String::new();
    let stdin = stdin();
    let mut reader = BufReader::new(stdin);
    reader
        .read_line(&mut bootstrap)
        .await
        .expect("Could not read bootstrap server address");

    let topic = IdBytes::random();

    let announce = {
        let topic = topic.clone();
        let bootstrap = bootstrap.clone();
        task::spawn(async move {
            let mut node = HyperDht::with_config(
                DhtConfig::default()
                    .ephemeral()
                    .set_bootstrap_nodes(&[&bootstrap]),
            )
            .await
            .expect("could not start");
            node.next().await;

            let query = QueryOpts::new(topic).port(12345);
            node.announce(query);
            node.next().await;
        })
    };

    let lookup = {
        let topic = topic.clone();
        let bootstrap = bootstrap.clone();
        task::spawn(async move {
            let mut node = HyperDht::with_config(
                DhtConfig::default()
                    .ephemeral()
                    .set_bootstrap_nodes(&[&bootstrap]),
            )
            .await
            .expect("could not start");
            node.next().await;

            let query = QueryOpts::new(topic.clone());
            node.lookup(query);
            let event = node.next().await;

            match event {
                Some(hyperswarm_dht::HyperDhtEvent::LookupResult { lookup, .. }) => {
                    println!("{:?}", lookup.all_peers().collect::<Vec<_>>());
                }
                _ => {}
            };

            let query = QueryOpts::new(topic).port(12345);
            node.next().await;
            node.unannounce(query);
        })
    };

    announce.await?;
    lookup.await?;
    Ok(())
}
