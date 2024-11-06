//! udx/dht-rpc internnals
use std::net::{IpAddr, Ipv4Addr};

use compact_encoding::State;
/**
 * from: https://github.com/holepunchto/dht-rpc/blob/bfa84ec5eef4cf405ab239b03ab733063d6564f2/lib/io.js#L424-L453
*/

#[derive(Debug, Clone)]
#[repr(usize)]
enum Command {
    Ping = 0,
    PingNat,
    FindNode,
    DownHint,
}

#[derive(Debug)]
struct To {
    id: Vec<u8>,
    host: Ipv4Addr,
    port: u16,
}

#[derive(Debug)]
struct Request {
    id: bool,
    command: Command,
    target: Option<Vec<u8>>,
    internal: bool,
    tid: u16,
    table_id: Option<Vec<u8>>,
}

const REQUEST_ID: u8 = 3;
use crate::Error;

impl Request {
    pub(crate) fn encode_request(
        &self,
        token: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        to: To,
    ) -> Result<Vec<u8>, crate::Error> {
        let id = false;
        //let mut state = State::new(0, 1 + 1 + 6 + 2, Vec::new());
        let mut state = State::new();
        let res = state.add_end(1 + 1 + 6 + 2);

        if id {
            state
                .add_end(32)
                .map_err(|e| Error::CompactEncodingErr(e.to_string()))?;
        }
        if token.is_some() {
            state.add_end(32).map_err(Error::from)?;
        }

        let cmd = self.command.clone() as usize;
        state.preencode_usize_var(&cmd)?;

        if self.target.is_some() {
            state.add_end(32)?;
        }

        if let Some(v) = &value {
            state.preencode_buffer(v)?;
        }

        let mut buff = state.create_buffer();

        buff[state.start()] = REQUEST_ID;
        state.add_start(1)?;

        if id {
            buff[state.start()] |= 1 << 0;
        }
        if token.is_some() {
            buff[state.start()] |= 1 << 1;
        }
        if self.internal {
            buff[state.start()] |= 1 << 2;
        }
        if self.target.is_some() {
            buff[state.start()] |= 1 << 3;
        }
        if value.is_some() {
            buff[state.start()] |= 1 << 4;
        }
        state.add_start(1)?;

        let [first, second] = self.tid.to_le_bytes();
        buff[state.start()] = first;
        state.add_start(1)?;
        buff[state.start()] = second;
        state.add_start(1)?;

        // TODO encode "to" arg
        // peer.ipv4.encode(state, to)
        // 4 bytes for ip
        let ip_bytes = to.host.octets();
        buff[state.start()] = ip_bytes[0];
        state.add_start(1)?;
        buff[state.start()] = ip_bytes[1];
        state.add_start(1)?;
        buff[state.start()] = ip_bytes[2];
        state.add_start(1)?;
        buff[state.start()] = ip_bytes[3];
        state.add_start(1)?;

        // port
        let [p1, p2] = to.port.to_le_bytes();
        buff[state.start()] = p1;
        state.add_start(1)?;
        buff[state.start()] = p2;
        state.add_start(1)?;

        if id {
            let table_id = todo!();
            state.encode_fixed_32(table_id, &mut buff);
        }
        if let Some(t) = token {
            // c.fixed32.encode(state, token)
            state.encode_fixed_32(&t, &mut buff)?;
        }

        buff[state.start()] = self.command.clone() as u8;
        state.add_start(1)?;

        // c.uint.encode(state, this.command)
        if let Some(t) = &self.target {
            // c.fixed32.encode(state, this.target)
            state.encode_fixed_32(&t, &mut buff)?;
        }
        if let Some(v) = value {
            state.encode_buffer(&v, &mut buff)?;
        }

        return Ok(buff.into());
    }
}

/// This data and expected result are taken from the initial message a node sends to a bootstrap
/// node in dht-rpc 6.15.1
#[test]
fn test_encode_buffer() -> Result<(), Box<dyn std::error::Error>> {
    let id = false;
    let command = Command::FindNode;
    let target = Some(vec![
        235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96, 17, 175, 157, 204, 216, 8, 191, 189, 16,
        140, 146, 202, 172, 84, 232, 73, 218, 113, 136, 161, 173,
    ]);
    let table_id = Some(vec![
        235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96, 17, 175, 157, 204, 216, 8, 191, 189, 16,
        140, 146, 202, 172, 84, 232, 73, 218, 113, 136, 161, 173,
    ]);
    let internal = true;
    let tid = 50632;
    let expected_buffer = vec![
        3, 12, 200, 197, 127, 0, 0, 1, 17, 39, 2, 235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96,
        17, 175, 157, 204, 216, 8, 191, 189, 16, 140, 146, 202, 172, 84, 232, 73, 218, 113, 136,
        161, 173,
    ];
    let req = Request {
        id,
        command,
        target,
        table_id,
        internal,
        tid,
    };

    let to = To {
        id: Vec::new(),
        host: "127.0.0.1".parse().unwrap(),
        port: 10001,
    };
    let res = req.encode_request(None, None, to)?;
    assert_eq!(res, expected_buffer);
    Ok(())
}
