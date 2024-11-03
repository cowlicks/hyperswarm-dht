use rusty_nodejs_repl::{
    pipe::{rust_js_stream, RsJsStreamBuilder},
    ConfigBuilder,
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    spawn,
    time::{sleep, timeout},
};

use futures::StreamExt;

use crate::{
    rpc::{RpcDht, RpcDhtEvent},
    test::free_port,
    DhtConfig,
};

const PATH_NEW_JS_NODE_MODULES: &str = "~/git/hyper/hyperswarm-dht/new_js_version/node_modules";

#[tokio::test]
async fn wip_bootstrap_rs_js() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let js1_port = free_port().unwrap();
    let ip = Ipv4Addr::from_str("127.0.0.1")?;
    let mut repl = ConfigBuilder::default()
        .path_to_node_modules(Some(PATH_NEW_JS_NODE_MODULES.into()))
        .imports(vec!["DHT = require('dht-rpc');".to_string()])
        .build()?
        .start()
        .await?;

    let _res = repl
        .run(&format!(
            "
node = await DHT.bootstrapper({js1_port}, '127.0.0.1', {{ host: '127.0.0.1', ephemeral: false }});
"
        ))
        .await?;

    let conf = RsJsStreamBuilder::default().build()?;
    let mut stream = dbg!(rust_js_stream(&mut repl, &conf).await)?;
    p(&repl
        .run(
            "
node.on('request', (r) => {
    console.log('request');
    socket.write('request');
})
node.on('ready', (r) => {
    console.log('ready');
    socket.write('ready');
})
node.on('bootstrap', (r) => {
    console.log('bootstrap');
    socket.write('bootstrap');
})
await node.fullyBootstrapped();
socket.write('ran');
console.log('ran');
",
        )
        .await?);
    dbg!();
    let js_addr = SocketAddr::from((ip, js1_port));
    let confb: DhtConfig = Default::default();
    let rs_port = free_port().unwrap();
    let confb = confb
        .set_ephemeral(false)
        .bind(("127.0.0.1", rs_port))
        .await
        .unwrap();

    let confb = confb.set_bootstrap_nodes(&[js_addr]);
    let mut b_node = RpcDht::with_config(confb).await?;
    b_node.bootstrap();
    spawn(async move {
        loop {
            if let Ok(Some(b_evt)) = timeout(Duration::from_millis(1500), b_node.next()).await {
                println!("B = {b_evt:?}");
                match b_evt {
                    RpcDhtEvent::RequestResult(_res) => println!("request result"),
                    RpcDhtEvent::ResponseResult(_) => println!("response result"),
                    RpcDhtEvent::RoutingUpdated { .. } => println!("routing updated"),
                    RpcDhtEvent::QueryResult { .. } => println!("query result"),
                    RpcDhtEvent::Bootstrapped { .. } => {}
                }
            }

            dbg!(&b_node.kbuckets);
            for e in b_node.kbuckets.iter() {
                dbg!(e);
            }
        }
    });
    let mut buf = vec![0; 3];
    let _s = stream.read(&mut buf).await?;
    p(&buf);
    p(&repl.drain_stdout().await?);
    p(&repl.drain_stderr().await?);
    sleep(Duration::from_millis(700)).await;
    let js2_port = free_port().unwrap();

    let run_str = format!(
        "
node2 = await new DHT({{
    bootstrap: [
        'localhost:{js1_port}',
    ],
    host: '127.0.0.1', ephemeral: false,
    nodes: [
        {{ host: '127.0.0.1', port: {rs_port} }},
        ],
}});
console.log('node2 made');
await node2.fullyBootstrapped();
console.log('node2 booted');

"
    );

    let r = repl.run(&run_str).await?;
    p(&r);
    dbg!(&js1_port);
    dbg!(&js2_port);
    dbg!(&rs_port);
    let c = format!(
        "
await node2.ping({{ host: '127.0.0.1', port: {js1_port} }});
// this times out :(
// maybe because of a udp vs udx thing
//await node2.ping({{ host: '127.0.0.1', port: {rs_port} }});
console.log(node2.toArray())
console.log(node.toArray())
   "
    );
    println!("{c}");
    let r = repl.run(&c).await?;
    p(&r);
    sleep(Duration::from_millis(700)).await;
    Ok(())
}

fn p(x: &[u8]) {
    println!("{}", String::from_utf8_lossy(x));
}
