use std::{net::ToSocketAddrs, sync::OnceLock};

use futures::StreamExt;

use crate::{
    kbucket::Key,
    udx::{
        smod::{DhtConfig, RpcDht},
        Command, InternalCommand,
    },
    IdBytes, DEFAULT_BOOTSTRAP,
};

#[allow(unused)]
pub fn log() {
    static START_LOGS: OnceLock<()> = OnceLock::new();
    START_LOGS.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_line_number(true)
            .without_time()
            // Reads `RUST_LOG` environment variable
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    });
}

#[tokio::test]
async fn t() -> crate::Result<()> {
    log();
    let conf = DhtConfig::default()
        .add_bootstrap_node(DEFAULT_BOOTSTRAP[0].to_socket_addrs()?.last().unwrap());

    let mut rpc = RpcDht::with_config(conf).await?;
    rpc.bootstrap();

    let mut i = 0;
    loop {
        let x = rpc.next().await;
        //dbg!(x);
        dbg!(i);
        if i == 3 {
            i += 1;
            break;
        }
        i += 1;
    }
    let target: [u8; 32] = [
        63, 63, 55, 18, 215, 204, 29, 175, 17, 182, 238, 196, 140, 156, 151, 174, 81, 108, 176,
        148, 18, 92, 110, 122, 174, 0, 158, 250, 102, 181, 105, 250,
    ];
    rpc.query(
        Command::Internal(InternalCommand::FindNode),
        Key::from(IdBytes(target)),
        None,
    );
    loop {
        let x = rpc.next().await;
        //dbg!(x);
        dbg!(i);
        i += 1;
        if i == 6 {
            break;
        }
    }

    Ok(())
}
