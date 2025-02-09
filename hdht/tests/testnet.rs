mod common;
use std::{net::SocketAddr, time::Duration};

use common::{
    js::{make_repl, KEYPAIR_JS},
    Result,
};
use dht_rpc::DhtConfig;
use futures::StreamExt;
use hyperdht::{crypto::Keypair2, HyperDht, HyperDhtEvent, QueryOpts};
use rusty_nodejs_repl::Repl;
use tracing_subscriber::EnvFilter;
pub fn log() {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_line_number(true)
        // print when instrumented funtion enters
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ENTER)
        .with_file(true)
        .with_env_filter(EnvFilter::from_default_env()) // Reads `RUST_LOG` environment variable
        .without_time()
        .init();
}

async fn other_node_socketadd(repl: &mut Repl) -> Result<SocketAddr> {
    Ok(repl
        .json_run::<String, _>(
            "
bs_node = testnet.nodes[1]
write(stringify(`${bs_node.host}:${bs_node.port}`))
",
        )
        .await?
        .parse()?)
}

#[tokio::test]
async fn testnet_lookup() -> Result<()> {
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

    let bs_addr = other_node_socketadd(&mut repl).await?;
    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;
    while let Some(_x) = hdht.next().await {
        if 2 * 2 == 4 {
            break;
        }
    }

    let topic: [u8; 32] = repl
        .json_run(
            "
    const b4a = require('b4a')
    topic = b4a.alloc(32);
    topic.write('hello', 0);
    write(stringify([...topic]))
    ",
        )
        .await?;
    log();

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
            Some(HyperDhtEvent::LookupResponse(resp)) => {
                if let Some(x) = resp.peers.last() {
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

    let bs_addr = other_node_socketadd(&mut repl).await?;

    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;
    while let Some(_x) = hdht.next().await {
        if 2 * 2 == 4 {
            break;
        }
    }

    let topic: [u8; 32] = repl
        .json_run(
            "
    const b4a = require('b4a')
    topic = b4a.alloc(32);
    topic.write('hello', 0);
    write(stringify([...topic]))
    ",
        )
        .await?;

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

#[tokio::test]
async fn verify_sign_announce() -> Result<()> {
    let mut repl = make_repl().await;
    repl.run(KEYPAIR_JS).await?;

    repl.run(
        "
createTestnet = require('hyperdht/testnet.js');
testnet = await createTestnet();
",
    )
    .await?;

    let bs_addr = other_node_socketadd(&mut repl).await?;
    let mut hdht = HyperDht::with_config(DhtConfig::default().add_bootstrap_node(bs_addr)).await?;

    let topic: [u8; 32] = repl
        .json_run(
            "
    const b4a = require('b4a')
    topic = b4a.alloc(32);
    topic.write('hello', 0);
    write(stringify([...topic]))
    ",
        )
        .await?;

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
