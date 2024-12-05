use futures::StreamExt;
use std::{net::TcpListener, time::Duration};

use crate::{
    udx::{RpcDht, RpcDhtEvent},
    DhtConfig, Result,
};
pub fn free_port() -> Option<u16> {
    match TcpListener::bind(("127.0.0.1", 0)) {
        Ok(listener) => {
            // Get the port number that was assigned
            match listener.local_addr() {
                Ok(addr) => Some(addr.port()),
                Err(_) => None,
            }
        }
        Err(_) => None,
    }
}

#[tokio::test]
async fn wip_bootstrap() -> Result<()> {
    let confa: DhtConfig = Default::default();
    let confa = confa
        .bind(("127.0.0.1", dbg!(free_port().unwrap())))
        .unwrap();
    let mut a_node = RpcDht::with_config(confa).await?;

    a_node.bootstrap();
    let a_addr = a_node.local_addr()?;

    tokio::task::spawn(async move {
        loop {
            if let Some(a_evt) = a_node.next().await {
                println!("A = {a_evt:?}");
                match a_evt {
                    RpcDhtEvent::RequestResult(_res) => println!("request result"),
                    RpcDhtEvent::ResponseResult(_) => println!("response result"),
                    RpcDhtEvent::RoutingUpdated { .. } => println!("routing updated"),
                    RpcDhtEvent::QueryResult { .. } => println!("query result"),
                    RpcDhtEvent::Bootstrapped { .. } => {}
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let confb: DhtConfig = Default::default();
    let confb = confb
        .bind(("127.0.0.1", dbg!(free_port().unwrap())))
        .unwrap();

    let confb = confb.set_bootstrap_nodes(&[a_addr]);
    let mut b_node = RpcDht::with_config(confb).await?;
    b_node.bootstrap();
    loop {
        if let Ok(Some(b_evt)) =
            tokio::time::timeout(Duration::from_millis(1500), b_node.next()).await
        {
            println!("B = {b_evt:?}");
            match b_evt {
                RpcDhtEvent::RequestResult(_res) => println!("request result"),
                RpcDhtEvent::ResponseResult(_) => println!("response result"),
                RpcDhtEvent::RoutingUpdated { .. } => println!("routing updated"),
                RpcDhtEvent::QueryResult { .. } => {
                    println!("query result");
                    break;
                }
                RpcDhtEvent::Bootstrapped { .. } => {}
            }
        }
    }

    let mut nodes = vec![];
    for n in b_node.kbuckets.iter() {
        dbg!(&n);
        nodes.push(n);
    }
    dbg!(nodes.len());
    Ok(())
}
