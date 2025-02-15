#![allow(unused, unreachable_code)]
mod common;
use std::{net::SocketAddr, time::Duration};

use common::{
    js::{make_repl, KEYPAIR_JS},
    log, Result,
};
use dht_rpc::DhtConfig;
use futures::StreamExt;
use hyperdht::{crypto::Keypair2, HyperDht, HyperDhtEvent};
use rusty_nodejs_repl::Repl;
use tracing_subscriber::EnvFilter;

fn show_bytes<T: AsRef<[u8]>>(x: T) {
    println!("{}", String::from_utf8(x.as_ref().to_vec()).unwrap())
}

macro_rules! poll_until {
    ($hdht:tt, $variant:path) => {{
        let res = loop {
            match $hdht.next().await {
                Some($variant(x)) => break x,
                _ => {}
            }
        };
        res
    }};
}

struct Testnet {
    pub repl: Repl,
}

impl Testnet {
    async fn new() -> Result<Self> {
        let mut repl = make_repl().await;
        repl.run(
            "
createTestnet = require('hyperdht/testnet.js');
testnet = await createTestnet();
",
        )
        .await?;
        Ok(Self { repl })
    }

    /// Get the address of a node from an existing testnet in JS
    /// NB: `testnet` must exist in the js context already
    async fn get_node_i_address(&mut self, node_index: usize) -> Result<SocketAddr> {
        Ok(self
            .repl
            .json_run::<String, _>(format!(
                "
bs_node = testnet.nodes[{node_index}]
write(stringify(`${{bs_node.host}}:${{bs_node.port}}`))
"
            ))
            .await?
            .parse()?)
    }
    /// Create a target/topic. whith the argument `topic` written to to the beggining of the buffer,
    /// and padded with zeros. The variable in js is named "topic"
    async fn make_topic(&mut self, topic: &str) -> Result<[u8; 32]> {
        Ok(self
            .repl
            .json_run(format!(
                "
    const b4a = require('b4a')
    topic = b4a.alloc(32);
    topic.write('{topic}', 0);
    write(stringify([...topic]))
    "
            ))
            .await?)
    }
}
/// Do an "announce" from javascript, then have rust "lookup"
/// In rust we get a public key in the lookup response, we check this is the same as the public key
/// of the node that announced.
#[tokio::test]
async fn js_announces_rs_looksup() -> Result<()> {
    let mut tn = Testnet::new().await?;

    let bs_addr = tn.get_node_i_address(1).await?;
    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;

    let topic = tn.make_topic("hello").await?;

    let _res = tn
        .repl
        .run(
            "
ann_node = testnet.nodes[testnet.nodes.length - 1];
query = await ann_node.announce(topic, ann_node.defaultKeyPair);
await query.finished();
    ",
        )
        .await?;

    let _qid = hdht.lookup(topic.into(), hyperdht::Commit::No);

    let mut r = None;
    loop {
        match hdht.next().await {
            Some(HyperDhtEvent::LookupResult(res)) => {
                if res.query_id == _qid {
                    break;
                }
            }
            Some(HyperDhtEvent::LookupResponse(resp)) => {
                if let Some(t) = resp.response.response.token {
                    r = Some(resp.peers);
                }
            }
            Some(_) => {}
            None => panic!("when would this end?"),
        }
    }

    let Some(peers) = r else {
        panic!();
    };
    let js_pk: Vec<u8> = tn
        .repl
        .json_run("writeJson([...ann_node.defaultKeyPair.publicKey])")
        .await?;
    assert_eq!(peers[0].public_key.as_slice(), js_pk);
    Ok(())
}

/// Do an "announce" from javascript, then have rust "lookup"
/// In rust we get a public key in the lookup response, we check this is the same as the public key
/// of the node that announced.
#[tokio::test]
async fn rs_announces_js_looksup() -> Result<()> {
    let mut tn = Testnet::new().await?;

    let bs_addr = tn.get_node_i_address(1).await?;
    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;

    let topic = tn.make_topic("hello").await?;
    let kp = Keypair2::default();
    let qid = hdht.announce(topic.into(), &kp, &[]);

    /// Run announce to completion
    let _res = poll_until!(hdht, HyperDhtEvent::AnnounceResult);
    /// do lookup in js.
    /// get result for js and show it matches the RS keypair above
    let found_pk_js: Vec<u8> = tn
        .repl
        .json_run(
            "
lookup_node = testnet.nodes[testnet.nodes.length - 1];
query = await lookup_node.lookup(topic);
for await (const x of query) {
    writeJson([...x.peers[0].publicKey]);
    break
}
",
        )
        .await?;

    assert_eq!(kp.public.as_slice(), found_pk_js);
    Ok(())
}
