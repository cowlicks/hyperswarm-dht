use std::net::ToSocketAddrs;

use futures::StreamExt;

use crate::{
    udx::smod::{DhtConfig, RpcDht},
    DEFAULT_BOOTSTRAP,
};

#[tokio::test]
async fn t() -> crate::Result<()> {
    let mut conf = DhtConfig::default();
    let conf = conf.add_bootstrap_node(DEFAULT_BOOTSTRAP[0].to_socket_addrs()?.last().unwrap());

    let mut rpc = RpcDht::with_config(conf).await?;
    rpc.bootstrap();

    let x = rpc.next().await;
    dbg!(x);

    Ok(())
}
