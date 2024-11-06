//! udx/dht-rpc internnals
//!
use std::convert::{TryFrom, TryInto};
#[allow(unreachable_code, dead_code)]
use std::net::{IpAddr, Ipv4Addr};

use crate::{Error, Result};
use compact_encoding::{CompactEncoding, EncodingError, State};
/**
 * from: https://github.com/holepunchto/dht-rpc/blob/bfa84ec5eef4cf405ab239b03ab733063d6564f2/lib/io.js#L424-L453
*/

const ID_SIZE: usize = 32;
const HASH_SIZE: usize = 32;
const VERSION: u8 = 0b11;
const RESPONSE_ID: u8 = (0b0001 << 4) | VERSION;
const REQUEST_ID: u8 = (0b0000 << 4) | VERSION;

// TODO in js this is de/encoded with c.uint which is for a variable sized unsigned integer.
// but it is always one byte. We use a u8 instead of a usize here. So there is a limit on 256
// commands.
#[derive(Debug, Clone)]
#[repr(u8)]
enum Command {
    Ping = 0,
    PingNat,
    FindNode,
    DownHint,
}

impl TryFrom<u8> for Command {
    type Error = crate::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use Command::*;
        Ok(match value {
            0 => Ping,
            1 => PingNat,
            2 => FindNode,
            3 => DownHint,
            x => return Err(crate::Error::InvalidRpcCommand(x)),
        })
    }
}

#[derive(Debug)]
struct Addr {
    id: Vec<u8>,
    host: Ipv4Addr,
    port: u16,
}

#[derive(Debug)]
pub struct Request {
    // _io {
    //  ephemeral: bool,
    //  serverSocket: Socket,
    //  table.id: [u8: 32]
    // }
    id: bool,
    command: Command,
    target: Option<Vec<u8>>,
    internal: bool,
    tid: u16,
    table_id: Option<Vec<u8>>,
}

fn encode_ip(ip: &Ipv4Addr, buff: &mut [u8], state: &mut State) -> Result<()> {
    let ip_bytes = ip.octets();
    buff[state.start()] = ip_bytes[0];
    state.add_start(1)?;
    buff[state.start()] = ip_bytes[1];
    state.add_start(1)?;
    buff[state.start()] = ip_bytes[2];
    state.add_start(1)?;
    buff[state.start()] = ip_bytes[3];
    state.add_start(1)?;
    Ok(())
}

impl Request {
    pub fn encode_request(
        &self,
        token: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        to: Addr,
    ) -> Result<Vec<u8>> {
        let id = false;
        //let mut state = State::new(0, 1 + 1 + 6 + 2, Vec::new());
        let mut state = State::new();
        let res = state.add_end(1 + 1 + 6 + 2);

        if id {
            state.add_end(ID_SIZE)?;
        }
        if token.is_some() {
            state.add_end(32)?;
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

        // TODO add state.encode_u16 function to compact_encoding
        state.encode_u16(self.tid, &mut buff)?;

        state.encode(&to, &mut buff)?;

        if id {
            let table_id = todo!();
            state.encode_fixed_32(table_id, &mut buff)?;
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

pub struct Io {}

const TID_SIZE: usize = 32;

pub struct Reply {
    tid: [u8; TID_SIZE],
}
fn decode_reply(buff: &[u8], from: Addr, state: &mut State) -> Result<Reply> {
    let flags = buff[state.start()];
    let tid = state.decode_u16(buff)?;
    todo!()
}

impl Io {
    pub fn decode_message(self, buff: Vec<u8>, host: Ipv4Addr, port: u16) -> Result<Vec<u8>> {
        let mut state = State::new_with_start_and_end(1, buff.len());
        let from = Addr {
            id: Vec::new(),
            host,
            port,
        };
        match buff[0] {
            REQUEST_ID => {
                let res = self.decode_request(&buff, from, &mut state)?;
                todo!()
            }
            RESPONSE_ID => {
                todo!()
            }
            _ => todo!("eror"),
        }
    }

    pub fn decode_request(self, buff: &[u8], mut from: Addr, state: &mut State) -> Result<Request> {
        let flags = buff[state.start()];
        state.add_start(1)?;

        let tid = state.decode_u16(buff)?;

        let Addr { id, host, port } = state.decode(buff)?;

        let id = if flags & 1 > 0 {
            // TODO it'd be nice if this returned Box<[u8;32]>
            Some(state.decode_fixed_32(buff)?)
        } else {
            None
        };

        let token = if flags & 2 > 0 {
            Some(state.decode_fixed_32(buff)?)
        } else {
            None
        };
        let internal = (flags & 4) != 0;
        let command = Command::try_from(buff[state.start()])?;
        let target = if flags & 8 > 0 {
            Some(state.decode_fixed_32(buff)?)
        } else {
            None
        };
        let value = if flags & 16 > 0 {
            Some(state.decode_buffer(buff)?)
        } else {
            None
        };
        if let Some(id) = id {
            let id_32: [u8; ID_SIZE] = id
                .as_ref()
                .try_into()
                .map_err(Error::IncorrectMessageIdSize)?;
            if let Ok(valid_id) = validate_id(&id_32, &from) {
                from.id = valid_id
            }
        }
        Ok(Request {
            command,
            target: target.map(Into::into),
            internal,
            tid,
            id: todo!(),
            table_id: todo!(),
        })
    }
}

/*
function validateId(id, from) {
        const expected = peer.id(from.host, from.port)
        return b4a.equals(expected, id) ? expected : null
}
  const addr = out.subarray(0, 6)
  ipv4.encode(
    { start: 0, end: 6, buffer: addr },
    { host, port }
  )
  sodium.crypto_generichash(out, addr)
*/
fn generic_hash(input: &[u8]) -> Result<[u8; HASH_SIZE]> {
    let mut out = [0; HASH_SIZE];
    let ret = unsafe {
        libsodium_sys::crypto_generichash(
            out.as_mut_ptr(),
            out.len(),
            input.as_ptr(),
            input.len() as u64,
            std::ptr::null(),
            0,
        )
    };
    if ret != 0 {
        return Err(Error::LibSodiumGenericHashError(ret));
    }
    Ok(out)
}

impl CompactEncoding<Addr> for State {
    fn preencode(&mut self, value: &Addr) -> std::result::Result<usize, EncodingError> {
        Ok(self.add_end(6)?)
    }

    fn encode(
        &mut self,
        value: &Addr,
        buff: &mut [u8],
    ) -> std::result::Result<usize, EncodingError> {
        let ip_bytes = value.host.octets();
        buff[self.start()] = ip_bytes[0];
        self.add_start(1)?;
        buff[self.start()] = ip_bytes[1];
        self.add_start(1)?;
        buff[self.start()] = ip_bytes[2];
        self.add_start(1)?;
        buff[self.start()] = ip_bytes[3];
        self.add_start(1)?;

        self.encode_u16(value.port, buff)?;
        Ok(self.start())
    }

    fn decode(&mut self, buff: &[u8]) -> std::result::Result<Addr, EncodingError> {
        let ip_start = self.start();
        let [ip1, ip2, ip3, ip4, port_1, port_2] = buff[ip_start..(ip_start + 4 + 2)] else {
            todo!()
        };
        self.add_start(4 + 2)?;
        let host = Ipv4Addr::from([ip1, ip2, ip3, ip4]);
        let port = u16::from_le_bytes([port_1, port_2]);
        Ok(Addr {
            id: Vec::new(),
            host,
            port,
        })
    }
}
fn validate_id(id: &[u8; ID_SIZE], from: &Addr) -> Result<Vec<u8>> {
    let mut from_buff = vec![0; 6];
    let mut state = State::new_with_start_and_end(0, 6);
    encode_ip(&from.host, &mut from_buff, &mut state)?;
    state.encode_u16(from.port, &mut from_buff)?;
    let result = generic_hash(&from_buff)?;
    //libsodium_sys::crypto_generichash(out, outlen, in_, inlen, key, keylen)
    todo!()
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::Ipv4Addr;

    const HOST: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
    /// This data and expected result are taken from the initial message a node sends to a bootstrap
    /// node in dht-rpc 6.15.1
    fn mk_request() -> Request {
        let id = false;
        let command = Command::FindNode;
        let target = Some(vec![
            235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96, 17, 175, 157, 204, 216, 8, 191, 189,
            16, 140, 146, 202, 172, 84, 232, 73, 218, 113, 136, 161, 173,
        ]);
        let table_id = Some(vec![
            235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96, 17, 175, 157, 204, 216, 8, 191, 189,
            16, 140, 146, 202, 172, 84, 232, 73, 218, 113, 136, 161, 173,
        ]);
        let internal = true;
        let tid = 50632;

        Request {
            id,
            command,
            target,
            table_id,
            internal,
            tid,
        }
    }
    #[test]
    fn test_encode_buffer() -> Result<()> {
        let expected_buffer = vec![
            3, 12, 200, 197, 127, 0, 0, 1, 17, 39, 2, 235, 159, 119, 93, 35, 250, 85, 76, 120, 152,
            96, 17, 175, 157, 204, 216, 8, 191, 189, 16, 140, 146, 202, 172, 84, 232, 73, 218, 113,
            136, 161, 173,
        ];
        let req = mk_request();

        let to = Addr {
            id: Vec::new(),
            host: HOST,
            port: 10001,
        };
        let res = req.encode_request(None, None, to)?;
        assert_eq!(res, expected_buffer);
        Ok(())
    }

    /*
    #[tokio::test]
    async fn do_bootstrap() -> Result<()> {
        use async_udx::UdxSocket;

        dbg!();
        let socket = UdxSocket::bind("127.0.0.1:0").await?;
        dbg!();

        let req = mk_request();
        dbg!();
        let to = Addr {
            id: Vec::new(),
            host: HOST,
            port: 10001,
        };
        dbg!();
        let buffer = req.encode_request(None, None, to)?;
        dbg!();

        socket.send((HOST, 10001).into(), &buffer);
        dbg!();
        let res = socket.recv().await;
        dbg!(&res);
        todo!()
    }
    */
}
