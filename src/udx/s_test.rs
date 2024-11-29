use std::net::ToSocketAddrs;

use futures::StreamExt;

use crate::{
    udx::smod::{DhtConfig, RpcDht},
    DEFAULT_BOOTSTRAP,
};

#[tokio::test]
async fn t() -> crate::Result<()> {
    let conf = DhtConfig::default()
        .add_bootstrap_node(DEFAULT_BOOTSTRAP[0].to_socket_addrs()?.last().unwrap());

    let mut rpc = RpcDht::with_config(conf).await?;
    rpc.bootstrap();

    let mut i = 0;
    loop {
        let x = rpc.next().await;
        dbg!(x);
        dbg!(i);
        i += 1;
    }

    Ok(())
}
