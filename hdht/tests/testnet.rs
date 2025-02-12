mod common;
use std::{net::SocketAddr, time::Duration};

use common::{
    js::{make_repl, KEYPAIR_JS},
    log, Result,
};
use dht_rpc::DhtConfig;
use futures::StreamExt;
use hyperdht::{crypto::Keypair2, HyperDht, HyperDhtEvent, QueryOpts};
use rusty_nodejs_repl::Repl;
use tracing_subscriber::EnvFilter;

fn show_bytes<T: AsRef<[u8]>>(x: T) {
    println!("{}", String::from_utf8(x.as_ref().to_vec()).unwrap())
}
/// Get the address of a node from an existing testnet in JS
/// NB: `testnet` must exist in the js context already
async fn other_node_socketadd(repl: &mut Repl, node_index: usize) -> Result<SocketAddr> {
    Ok(repl
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
async fn make_topic(repl: &mut Repl, topic: &str) -> Result<[u8; 32]> {
    Ok(repl
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

#[tokio::test]
async fn testnet_lookup() -> Result<()> {
    let mut repl = make_repl().await;
    // Create testnet and get bootstrap address
    repl.run(
        "
createTestnet = require('hyperdht/testnet.js');
testnet = await createTestnet();
",
    )
    .await?;

    let bs_addr = other_node_socketadd(&mut repl, 1).await?;

    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;
    while let Some(_x) = hdht.next().await {
        if 2 * 2 == 4 {
            break;
        }
    }

    let topic = make_topic(&mut repl, "hello").await?;
    log();

    dbg!();
    let _res = repl
        .run(
            "
    ann_node = testnet.nodes[testnet.nodes.length - 1];
    query = await ann_node.announce(topic, ann_node.defaultKeyPair);
    await query.finished();
    ",
        )
        .await?;

    let _qid = hdht.lookup(topic.into(), hyperdht::Commit::No);

    let mut public_key = None;
    loop {
        match hdht.next().await {
            Some(HyperDhtEvent::LookupResult(res)) => {
                if res.query_id == _qid {
                    break;
                }
            }
            Some(HyperDhtEvent::LookupResponse(res)) => {
                if let Some(x) = res.peers.last() {
                    let pk = public_key.get_or_insert(x.public_key.clone());
                    assert_eq!(*pk, x.public_key);
                }
            }
            Some(_) => {}
            None => panic!("when would this end?"),
        }
    }

    assert!(public_key.is_some());
    Ok(())
}

#[tokio::test]
async fn net_announce() -> Result<()> {
    let mut repl = make_repl().await;
    repl.run(KEYPAIR_JS).await?;
    // Create testnet and get bootstrap address
    repl.run(
        "
createTestnet = require('hyperdht/testnet.js');
testnet = await createTestnet();
",
    )
    .await?;

    let bs_addr = other_node_socketadd(&mut repl, 1).await?;
    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;

    while let Some(_x) = hdht.next().await {
        if 2 * 2 == 4 {
            break;
        }
    }

    let topic = make_topic(&mut repl, "hello").await?;

    let kp = Keypair2::default();
    let opts = QueryOpts::default();
    let qid = hdht.announce(topic.into(), &kp, &[], &opts);

    log();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let what = repl.drain_stdout().await.unwrap();
            let what_str = String::from_utf8(what).unwrap();
            println!("JS STDOUT: {what_str}");
        }
    });
    loop {
        match hdht.next().await {
            Some(e) => {
                dbg!(&e);
            }
            None => panic!("when would this end?"),
        }
    }

    Ok(())
}

/// Do an "announce" from javascript, then have rust "lookup"
/// In rust we get a public key in the lookup response, we check this is the same as the public key
/// of the node that announced.
#[tokio::test]
async fn js_announces_rs_looksup() -> Result<()> {
    let mut repl = make_repl().await;

    repl.run(
        "
createTestnet = require('hyperdht/testnet.js');
testnet = await createTestnet();
",
    )
    .await?;

    let bs_addr = other_node_socketadd(&mut repl, 1).await?;
    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;

    let topic = make_topic(&mut repl, "hello").await?;

    let _res = repl
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
                dbg!();
                if res.query_id == _qid {
                    break;
                }
            }
            Some(HyperDhtEvent::LookupResponse(resp)) => {
                if let Some(t) = resp.response.token {
                    r = Some(resp.response.clone());
                }
            }
            Some(x) => {
                //dbg!(x);
            }
            None => panic!("when would this end?"),
        }
    }

    assert!(r.is_some());

    r.inspect(|x| {
        //dbg!(x);
    });

    /*
        // in rust create the parts of the announce request
        // token - usually you get this in a reply to a request. What kinda req?
        //   a valid token will be included in a lookup request
        // could just log the parts from rust. See QueryStreamType::commit
        // components we need
        let x = repl
            .run(
                "
    Persistent = require('hyperdht/lib/persistent.js');
    write(Persistent.prototype.verifyAnnounce.toString())
    //writeJson(Persistent.verifyAnnounce({ value, target, token, }, nodeId))
    ",
            )
            .await?;


        println!("{}", String::from_utf8_lossy(&x));

    */
    todo!()
}
