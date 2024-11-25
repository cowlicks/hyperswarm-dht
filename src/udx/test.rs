use crate::{
    kbucket::{KBucketsTable, KeyBytes},
    udx::{
        io::{Io, Reply, Request},
        thirty_two_random_bytes, Addr, Command, InternalCommand, RpcDhtBuilder,
    },
    Result, DEFAULT_BOOTSTRAP,
};
use async_udx::UdxSocket;
use compact_encoding::State;
use std::{
    cell::RefCell,
    convert::TryInto,
    net::{Ipv4Addr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use super::cenc::validate_id;
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
        for addr in bs.to_socket_addrs().unwrap() {
            let mut rpc = RpcDhtBuilder::default().add_bootstrap_node(addr)?.build()?;
            let rec = rpc.bootstrap().await?;
            dbg!(rec);
        }
    }
    Ok(())
}

#[tokio::test]
async fn ping_global() -> Result<()> {
    let addr = DEFAULT_BOOTSTRAP[0]
        .to_socket_addrs()
        .unwrap()
        .last()
        .unwrap();
    let id = thirty_two_random_bytes();
    let key = KeyBytes::new(id);
    let kbuckets = KBucketsTable::new(
        key,
        Duration::from_secs(DEFAULT_KBUCKET_PENDING_TIMEOUT_SECS),
    );
    let rpc = RpcDhtBuilder::default()
        .kbuckets(kbuckets)
        .add_bootstrap_node(addr)?
        .build()?;
    let _rec = rpc.ping(&addr).await?;
    Ok(())
}

const DEFAULT_KBUCKET_PENDING_TIMEOUT_SECS: u64 = 300;

#[tokio::test]
async fn bootstrap_global() -> Result<()> {
    let addr = DEFAULT_BOOTSTRAP[0]
        .to_socket_addrs()
        .unwrap()
        .last()
        .unwrap();
    let id = thirty_two_random_bytes();
    let key = KeyBytes::new(id);
    let kbuckets = KBucketsTable::new(
        key,
        Duration::from_secs(DEFAULT_KBUCKET_PENDING_TIMEOUT_SECS),
    );
    let mut rpc = RpcDhtBuilder::default()
        .id(Arc::new(RefCell::new(id)))
        .add_bootstrap_node(addr)?
        .kbuckets(kbuckets)
        .build()?;
    let rec = rpc.bootstrap().await?;
    dbg!(rec);
    Ok(())
}

// This data and expected result are taken from the initial message a node sends to a bootstrap
// node in dht-rpc 6.15.1
fn mk_request() -> Request {
    Request {
        to: Addr {
            id: None,
            host: HOST,
            port: BOOTSTRAP_PORT,
        },
        from: Some(Addr {
            id: None,
            host: HOST,
            port: 12345,
        }),
        command: InternalCommand::FindNode.into(),
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
    let io = Io::new(Arc::new(RefCell::new(thirty_two_random_bytes())))?;
    let ping_req = io.create_ping(&BOOTSTRAP_ADDR);
    let buff = ping_req.encode(&io, false)?;
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
    let io = Io::new(Arc::new(RefCell::new(thirty_two_random_bytes())))?;

    let res = req.encode(&io, false)?;
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
    let res = Request::decode(&from, &buff, &mut state)?;
    from.id = res.from.as_ref().unwrap().id.clone();
    let expected = Request {
        tid,
        from: Some(from),
        to,
        token,
        internal,
        command: Command::Internal(command.try_into()?),
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
    let id = [
        233, 110, 1, 70, 163, 62, 2, 41, 135, 254, 37, 108, 99, 67, 172, 93, 32, 219, 164, 126,
        161, 182, 52, 31, 147, 41, 3, 180, 132, 105, 69, 62,
    ];
    let from = Addr {
        id: Some(id.clone().try_into().unwrap()),
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
    let token: Option<[u8; 32]> = None;
    let closer_nodes: Vec<Addr> = vec![Addr {
        id: None,
        host: HOST,
        port: 39361,
    }];
    let error = 0;
    let value: Option<Vec<u8>> = None;

    let mut state = State::new_with_start_and_end(state_start, state_end);
    let result = Reply::decode(&from, &state_buff, &mut state)?;

    let expected = Reply {
        tid,
        rtt: 0,
        from,
        to,
        token,
        closer_nodes,
        error,
        value,
    };
    assert_eq!(result, expected);
    Ok(())
}

#[test]
fn test_validate_id() -> Result<()> {
    let id: [u8; 32] = [
        128, 153, 111, 213, 115, 62, 11, 125, 92, 62, 223, 183, 3, 135, 211, 39, 152, 41, 73, 55,
        160, 113, 55, 48, 114, 90, 50, 44, 201, 131, 192, 94,
    ];
    let from = Addr {
        id: None,
        host: Ipv4Addr::new(188, 166, 28, 20),
        port: 60692,
    };
    assert!(validate_id(&id, &from).is_some());
    Ok(())
}

#[test]
fn test_decode_reply2() -> Result<()> {
    let id: [u8; 32] = [
        150, 215, 52, 186, 21, 138, 59, 143, 151, 102, 157, 243, 44, 164, 119, 219, 83, 170, 212,
        81, 106, 47, 82, 219, 19, 156, 62, 133, 43, 118, 112, 239,
    ];

    let from = Addr {
        id: None,
        host: Ipv4Addr::new(194, 113, 75, 189),
        port: 55922,
    };
    let buff: Vec<u8> = vec![
        19, 1, 255, 87, 23, 136, 216, 2, 190, 175, 150, 215, 52, 186, 21, 138, 59, 143, 151, 102,
        157, 243, 44, 164, 119, 219, 83, 170, 212, 81, 106, 47, 82, 219, 19, 156, 62, 133, 43, 118,
        112, 239,
    ];

    let reply = Reply {
        tid: 22527,
        rtt: 0,
        from: Addr {
            id: Some(id),
            host: Ipv4Addr::new(194, 113, 75, 189),
            port: 55922,
        },
        to: Addr {
            id: None,
            host: Ipv4Addr::new(23, 136, 216, 2),
            port: 44990,
        },
        token: None,
        closer_nodes: vec![],
        error: 0,
        value: None,
    };

    let mut state = State::new_with_start_and_end(1, buff.len());
    let result = Reply::decode(&from, &buff, &mut state)?;
    assert_eq!(result, reply);

    Ok(())
}
#[test]
fn test_decode_reply3() -> Result<()> {
    let id: [u8; 32] = [
        225, 49, 134, 54, 15, 155, 254, 45, 250, 31, 96, 115, 173, 142, 30, 238, 191, 178, 191,
        181, 97, 47, 89, 170, 82, 69, 188, 119, 101, 238, 137, 168,
    ];

    let host = Ipv4Addr::new(188, 166, 28, 20);
    let port = 33041;
    let from = Addr {
        id: None,
        host,
        port,
    };
    let buff: Vec<u8> = vec![
        19, 5, 255, 124, 74, 101, 11, 68, 57, 146, 225, 49, 134, 54, 15, 155, 254, 45, 250, 31, 96,
        115, 173, 142, 30, 238, 191, 178, 191, 181, 97, 47, 89, 170, 82, 69, 188, 119, 101, 238,
        137, 168, 20, 188, 166, 28, 20, 206, 234, 138, 68, 147, 8, 169, 223, 188, 166, 28, 20, 43,
        213, 170, 187, 185, 104, 2, 132, 188, 166, 28, 20, 67, 183, 188, 166, 28, 20, 179, 213,
        188, 166, 28, 20, 104, 149, 188, 166, 28, 20, 157, 178, 188, 166, 28, 20, 78, 217, 188,
        166, 28, 20, 79, 195, 188, 166, 28, 20, 188, 182, 103, 55, 9, 56, 73, 194, 188, 166, 28,
        20, 149, 130, 188, 166, 28, 20, 39, 206, 188, 166, 28, 20, 221, 216, 138, 68, 147, 8, 211,
        223, 188, 166, 28, 20, 230, 156, 188, 166, 28, 20, 137, 194, 188, 166, 28, 20, 73, 194,
        176, 9, 42, 203, 214, 211,
    ];
    let closer_nodes = vec![
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 60110,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(138, 68, 147, 8),
            port: 57257,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 54571,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(170, 187, 185, 104),
            port: 33794,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 46915,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 54707,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 38248,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 45725,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 55630,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 49999,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 46780,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(103, 55, 9, 56),
            port: 49737,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 33429,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 52775,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 55517,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(138, 68, 147, 8),
            port: 57299,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 40166,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 49801,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(188, 166, 28, 20),
            port: 49737,
        },
        Addr {
            id: None,
            host: Ipv4Addr::new(176, 9, 42, 203),
            port: 54230,
        },
    ];

    let reply = Reply {
        tid: 31999,
        rtt: 0,
        from: Addr {
            id: Some(id),
            host,
            port,
        },
        to: Addr {
            id: None,
            host: Ipv4Addr::new(74, 101, 11, 68),
            port: 37433,
        },
        token: None,
        closer_nodes,
        error: 0,
        value: None,
    };

    let mut state = State::new_with_start_and_end(1, buff.len());
    let result = Reply::decode(&from, &buff, &mut state)?;

    assert_eq!(result, reply);

    Ok(())
}

#[test]
fn test_just_reply_buff() -> Result<()> {
    let from = Addr {
        id: None,
        host: Ipv4Addr::new(2, 0, 194, 73),
        port: 25432,
    };
    let buff = vec![
        19, 5, 101, 186, 74, 101, 11, 68, 118, 171, 84, 212, 185, 198, 68, 16, 72, 6, 31, 128, 207,
        105, 86, 76, 238, 70, 165, 115, 10, 223, 205, 89, 251, 230, 192, 86, 187, 73, 175, 119, 21,
        70, 20, 188, 166, 28, 20, 104, 149, 150, 136, 237, 154, 210, 167, 150, 136, 237, 154, 237,
        205, 150, 136, 237, 154, 54, 160, 34, 154, 38, 202, 81, 195, 150, 136, 237, 154, 31, 134,
        150, 136, 237, 154, 62, 155, 150, 136, 237, 154, 247, 189, 136, 243, 5, 20, 165, 197, 150,
        136, 237, 154, 169, 204, 88, 99, 3, 86, 35, 144, 188, 166, 28, 20, 160, 131, 170, 187, 185,
        104, 2, 132, 150, 136, 237, 154, 206, 221, 24, 199, 122, 164, 215, 193, 170, 187, 185, 104,
        115, 236, 141, 147, 52, 146, 73, 194, 88, 99, 3, 86, 47, 195, 188, 166, 28, 20, 179, 213,
        136, 243, 5, 20, 80, 215,
    ];
    let mut state = State::new_with_start_and_end(1, buff.len());
    let _ = Reply::decode(&from, &buff, &mut state)?;
    Ok(())
}
