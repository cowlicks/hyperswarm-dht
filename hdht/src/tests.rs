use crypto::keypair;
use dht_rpc::DEFAULT_BOOTSTRAP;
use futures::StreamExt;

use super::*;

macro_rules! bootstrap_dht {
    () => {
        bootstrap_dht!(true)
    };
    ($eph:expr) => {{
        let mut bs = HyperDht::with_config(
            DhtConfig::default()
                .empty_bootstrap_nodes()
                .set_ephemeral($eph),
        )
        .await?;
        let addr = bs.local_addr()?;
        tokio::task::spawn(async move {
            loop {
                // process each incoming message
                bs.next().await;
            }
        });
        addr
    }};
}

#[ignore]
#[tokio::test]
async fn local_bootstrap() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // ephemeral node used for bootstrapping
    let bs_addr = bootstrap_dht!();

    // represents stateful nodes in the DHT
    let mut state =
        HyperDht::with_config(DhtConfig::default().set_bootstrap_nodes(&[&bs_addr])).await?;

    let (tx, rx) = futures::channel::oneshot::channel();

    tokio::task::spawn(async move {
        if let Some(HyperDhtEvent::Bootstrapped { .. }) = state.next().await {
            // after initial bootstrapping the state`s address is included in the bs`
            // routing table. Then the `node` can start to announce
            tx.send(()).expect("Failed to send");
        } else {
            panic!("expected bootstrap result first")
        }
        loop {
            state.next().await;
        }
    });

    let port = 12345;
    // announce options
    let opts = QueryOpts::new(IdBytes::random()).port(port);
    let target = IdBytes::random();
    let key_pair = keypair();
    let relay_addresses: Vec<SocketAddr> = DEFAULT_BOOTSTRAP
        .iter()
        .map(|bs| bs.to_socket_addrs().unwrap().last().unwrap())
        .collect();

    let opts2 = QueryOpts2 {
        clear: false,
        closest_nodes: vec![],
        only_closest_nodes: false,
    };
    let mut node = HyperDht::with_config(
        DhtConfig::default()
            .ephemeral()
            .set_bootstrap_nodes(&[bs_addr]),
    )
    .await?;

    // wait until `state` is bootstrapped
    rx.await?;

    let mut unannounced = false;
    loop {
        if let Some(event) = node.next().await {
            match event {
                HyperDhtEvent::Bootstrapped { .. } => {
                    // 1. announce topic and port
                    node.announce(target, &key_pair, &relay_addresses, &opts2);
                }
                HyperDhtEvent::AnnounceResult { .. } => {
                    // 2. look up the announced topic
                    node.lookup(opts.topic, Commit::No);
                }
                HyperDhtEvent::UnAnnounceResult { .. } => {
                    // 4. another lookup that now comes up empty
                    unannounced = true;
                    node.lookup(
                        opts.topic,
                        Commit::No, /* TODO is this the correct commit */
                    );
                }
                HyperDhtEvent::LookupResult { lookup, .. } => {
                    if unannounced {
                        // 5. after un announcing lookup yields zero peers
                        assert!(lookup.is_empty());
                        return Ok(());
                    }
                    assert_eq!(lookup.topic, opts.topic);
                    assert_eq!(lookup.len(), 1);
                    let remotes = lookup.remotes().cloned().collect::<Vec<_>>();
                    assert_eq!(remotes.len(), 1);

                    let node_addr = node.local_addr()?;
                    if let SocketAddr::V4(mut addr) = node_addr {
                        addr.set_port(port as u16);
                        assert_eq!(remotes[0], SocketAddr::V4(addr));
                    }
                    // 3. un announce the port
                    node.unannounce(opts.clone());
                }
                HyperDhtEvent::CustomCommandQuery { .. } => {
                    todo!()
                }
            }
        }
    }
}
