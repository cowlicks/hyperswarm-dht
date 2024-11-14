use crate::{
    udx::{
        cenc::{decode_reply, decode_request},
        io::{Io, Reply, Request},
        Addr, Command, RpcDhtBuilder,
    },
    Result, DEFAULT_BOOTSTRAP,
};
use async_udx::UdxSocket;
use compact_encoding::State;
use std::{
    convert::TryInto,
    net::{Ipv4Addr, SocketAddr, ToSocketAddrs},
    time::Duration,
};
const HOST: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const BOOTSTRAP_PORT: u16 = 10001;
const BOOTSTRAP_ADDR: Addr = Addr {
    id: None,
    host: HOST,
    port: BOOTSTRAP_PORT,
};

#[ignore]
#[tokio::test]
async fn bootstrap_local() -> Result<()> {
    let hosts = ["127.0.0.1:10001"];
    for bs in hosts {
        for addr in dbg!(bs.to_socket_addrs().unwrap()) {
            let rpc = RpcDhtBuilder::default().add_bootstrap_node(addr)?.build()?;
            let rec = rpc.bootstrap().await?;
            dbg!(rec);
        }
    }
    Ok(())
}

#[tokio::test]
async fn ping_global() -> Result<()> {
    for bs in DEFAULT_BOOTSTRAP {
        dbg!(&bs);
        for addr in dbg!(bs.to_socket_addrs().unwrap()) {
            let rpc = RpcDhtBuilder::default().add_bootstrap_node(addr)?.build()?;
            let rec = rpc.ping(&addr).await?;
            dbg!(rec);
        }
    }
    Ok(())
}

#[tokio::test]
async fn bootstrap_global() -> Result<()> {
    for bs in DEFAULT_BOOTSTRAP {
        for addr in dbg!(bs.to_socket_addrs().unwrap()) {
            let rpc = RpcDhtBuilder::default().add_bootstrap_node(addr)?.build()?;
            let rec = rpc.bootstrap().await?;
            dbg!(rec);
        }
    }
    Ok(())
}

// This data and expected result are taken from the initial message a node sends to a bootstrap
// node in dht-rpc 6.15.1
fn mk_request() -> Request {
    Request {
        to: Some(Addr {
            id: None,
            host: HOST,
            port: BOOTSTRAP_PORT,
        }),
        from: Some(Addr {
            id: None,
            host: HOST,
            port: 12345,
        }),
        command: Command::FindNode,
        target: Some([
            235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96, 17, 175, 157, 204, 216, 8, 191, 189,
            16, 140, 146, 202, 172, 84, 232, 73, 218, 113, 136, 161, 173,
        ]),
        token: None,
        value: None,

        internal: true,
        tid: 50632,
    }
}

#[ignore]
#[tokio::test]
async fn test_ping() -> Result<()> {
    let sock = UdxSocket::bind("127.0.0.1:0")?;
    let io = Io::new()?;
    let ping_req = io.create_ping(&BOOTSTRAP_ADDR);
    let buff = ping_req.encode_request(&io, false)?;
    println!("{buff:?}");
    //ping_req.en
    let sock_addr: SocketAddr = format!("{HOST}:{BOOTSTRAP_PORT}").parse().unwrap();
    sock.send(sock_addr, &buff);
    //socke.send(&B
    tokio::time::sleep(Duration::from_millis(1000)).await;
    Ok(())
}

#[tokio::test]
async fn test_encode_buffer() -> Result<()> {
    let expected_buffer = vec![
        3, 12, 200, 197, 127, 0, 0, 1, 17, 39, 2, 235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96,
        17, 175, 157, 204, 216, 8, 191, 189, 16, 140, 146, 202, 172, 84, 232, 73, 218, 113, 136,
        161, 173,
    ];
    let req = mk_request();
    let io = Io::new()?;

    let res = req.encode_request(&io, false)?;
    assert_eq!(res, expected_buffer);
    Ok(())
}

#[test]
fn test_decode_request() -> Result<()> {
    let state_start = 1;
    let state_end = 75;
    let buff = [
        3, 13, 38, 33, 127, 0, 0, 1, 17, 39, 186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50,
        6, 25, 133, 59, 149, 198, 162, 26, 109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
        2, 186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50, 6, 25, 133, 59, 149, 198, 162,
        26, 109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
    ];
    let _from_before = Addr {
        id: None,
        host: HOST,
        port: 45475,
    };
    let value = None;
    let target = [
        186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50, 6, 25, 133, 59, 149, 198, 162, 26,
        109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
    ];
    let command = 2;
    let internal = true;
    let token = None;
    let to = Addr {
        id: None,
        host: HOST,
        port: BOOTSTRAP_PORT,
    };
    let mut from = Addr {
        id: None,
        host: HOST,
        port: 45475,
    };
    let tid = 8486;
    let _id = [
        186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50, 6, 25, 133, 59, 149, 198, 162, 26,
        109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
    ];

    let mut state = State::new_with_start_and_end(state_start, state_end);
    let res = decode_request(&buff, from.clone(), &mut state)?;
    from.id = res.from.as_ref().unwrap().id.clone();
    let expected = Request {
        tid,
        from: Some(from),
        to: Some(to),
        token,
        internal,
        command: command.try_into()?,
        target: Some(target),
        value,
    };
    assert_eq!(res, expected);
    Ok(())
}

#[test]
fn test_decode_reply() -> Result<()> {
    let state_start = 1;
    let state_end = 49;
    let state_buff = vec![
        19, 5, 73, 32, 127, 0, 0, 1, 193, 153, 233, 110, 1, 70, 163, 62, 2, 41, 135, 254, 37, 108,
        99, 67, 172, 93, 32, 219, 164, 126, 161, 182, 52, 31, 147, 41, 3, 180, 132, 105, 69, 62, 1,
        127, 0, 0, 1, 193, 153,
    ];
    let from = Addr {
        id: None,
        host: HOST,
        port: BOOTSTRAP_PORT,
    };
    let _flags = 5;
    let tid = 8265;
    let to = Addr {
        id: None,
        host: HOST,
        port: 39361,
    };
    let _id = [
        233, 110, 1, 70, 163, 62, 2, 41, 135, 254, 37, 108, 99, 67, 172, 93, 32, 219, 164, 126,
        161, 182, 52, 31, 147, 41, 3, 180, 132, 105, 69, 62,
    ];
    let token: Option<[u8; 32]> = None;
    let closer_nodes: Vec<Addr> = vec![Addr {
        id: None,
        host: HOST,
        port: 39361,
    }];
    let error = 0;
    let value: Option<Vec<u8>> = None;
    //let validateId: Option<[u8; 32]> = Some([
    //    233, 110, 1, 70, 163, 62, 2, 41, 135, 254, 37, 108, 99, 67, 172, 93, 32, 219, 164, 126,
    //    161, 182, 52, 31, 147, 41, 3, 180, 132, 105, 69, 62,
    //]);

    let mut state = State::new_with_start_and_end(state_start, state_end);
    let mut og_from = from.clone();
    let result = decode_reply(&state_buff, from, &mut state)?;

    og_from.id = result.from.id.clone();
    let expected = Reply {
        tid,
        rtt: 0,
        from: og_from,
        to,
        token,
        closer_nodes: Some(closer_nodes),
        error,
        value,
    };
    assert_eq!(result, expected);
    Ok(())
}
