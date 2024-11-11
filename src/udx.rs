//! udx/dht-rpc internnals
//!
use rand::Rng;
#[allow(unreachable_code, dead_code)]
use std::net::Ipv4Addr;
use std::{
    convert::{TryFrom, TryInto},
    sync::atomic::{AtomicU16, Ordering},
};

use crate::{
    cenc::calculate_id,
    constants::{ID_SIZE, REQUEST_ID, RESPONSE_ID, TABLE_ID_SIZE},
    Result,
};
use compact_encoding::{CompactEncoding, State};
/**
 * from: https://github.com/holepunchto/dht-rpc/blob/bfa84ec5eef4cf405ab239b03ab733063d6564f2/lib/io.js#L424-L453
*/

// TODO in js this is de/encoded with c.uint which is for a variable sized unsigned integer.
// but it is always one byte. We use a u8 instead of a usize here. So there is a limit on 256
// commands.
#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum Command {
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

#[derive(Debug, PartialEq, Clone)]
pub struct Addr {
    pub id: Option<Vec<u8>>,
    pub host: Ipv4Addr,
    pub port: u16,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Reply {
    tid: u16,
    rtt: usize,
    from: Addr,
    to: Addr,
    token: Option<[u8; 32]>,
    closer_nodes: Option<Vec<Addr>>,
    error: u8,
    value: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct Request {
    tid: u16,
    from: Option<Addr>,
    to: Option<Addr>,
    token: Option<[u8; 32]>,
    internal: bool,
    command: Command,
    target: Option<[u8; 32]>,
    value: Option<Vec<u8>>,
}

impl Request {
    fn new(
        tid: u16,
        from: Option<Addr>,
        to: Option<Addr>,
        token: Option<[u8; 32]>,
        internal: bool,
        command: Command,
        target: Option<[u8; 32]>,
        value: Option<Vec<u8>>,
    ) -> Self {
        Self {
            tid,
            from,
            to,
            token,
            internal,
            command,
            target,
            value,
        }
    }
    fn get_table_id() -> [u8; 32] {
        todo!()
    }
    pub fn encode_request(&self, io: &Io, is_server_socket: bool) -> Result<Vec<u8>> {
        let id = !io.ephemeral && is_server_socket;
        let mut state = State::new();
        state.add_end(1 + 1 + 6 + 2)?;

        if is_server_socket {
            state.add_end(ID_SIZE)?;
        }
        if self.token.is_some() {
            state.add_end(32)?;
        }

        let cmd = self.command.clone() as usize;
        state.preencode_usize_var(&cmd)?;

        if self.target.is_some() {
            state.add_end(32)?;
        }

        if let Some(v) = &self.value {
            state.preencode_buffer(v)?;
        }

        let mut buff = state.create_buffer();

        buff[state.start()] = REQUEST_ID;
        state.add_start(1)?;

        if id {
            buff[state.start()] |= 1 << 0;
        }
        if self.token.is_some() {
            buff[state.start()] |= 1 << 1;
        }
        if self.internal {
            buff[state.start()] |= 1 << 2;
        }
        if self.target.is_some() {
            buff[state.start()] |= 1 << 3;
        }
        if self.value.is_some() {
            buff[state.start()] |= 1 << 4;
        }
        state.add_start(1)?;

        // TODO add state.encode_u16 function to compact_encoding
        state.encode_u16(self.tid, &mut buff)?;

        let Some(to) = self.to.as_ref() else {
            // Maybe Req should have *one* field for To/From that's just Addr
            todo!()
        };
        state.encode(to, &mut buff)?;

        if id {
            state.encode_fixed_32(&Request::get_table_id(), &mut buff)?;
        }
        if let Some(t) = self.token {
            // c.fixed32.encode(state, token)
            state.encode_fixed_32(&t, &mut buff)?;
        }

        buff[state.start()] = self.command.clone() as u8;
        state.add_start(1)?;

        // c.uint.encode(state, this.command)
        if let Some(t) = &self.target {
            // c.fixed32.encode(state, this.target)
            state.encode_fixed_32(t, &mut buff)?;
        }
        if let Some(v) = self.value.as_ref() {
            state.encode_buffer(&v, &mut buff)?;
        }

        return Ok(buff.into());
    }
}

pub struct Io {
    tid: AtomicU16,
    ephemeral: bool,
    //serverSocket: UdxSocket,
}

impl Default for Io {
    fn default() -> Self {
        Self {
            tid: AtomicU16::new(rand::thread_rng().gen()),
            ephemeral: true,
        }
    }
}

impl Io {
    pub fn create_ping(&self, to: &Addr) -> Request {
        self.create_request(to, None, true, Command::Ping, None, None)
    }

    pub fn create_ping_nat(&self, to: &Addr, value: Vec<u8>) -> Request {
        self.create_request(to, None, true, Command::PingNat, None, Some(value))
    }

    pub fn create_find_node(&self, to: &Addr, target: &[u8; 32]) -> Request {
        self.create_request(to, None, true, Command::Ping, Some(target.clone()), None)
    }

    pub fn create_request(
        &self,
        to: &Addr,
        token: Option<[u8; 32]>,
        internal: bool,
        command: Command,
        target: Option<[u8; 32]>,
        value: Option<Vec<u8>>,
    ) -> Request {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed);
        Request::new(
            tid,
            None,
            Some(to.clone()),
            token,
            internal,
            command,
            target,
            value,
        )
    }

    pub fn decode_message(self, buff: Vec<u8>, host: Ipv4Addr, port: u16) -> Result<Vec<u8>> {
        let mut state = State::new_with_start_and_end(1, buff.len());
        let from = Addr {
            id: None,
            host,
            port,
        };
        match buff[0] {
            REQUEST_ID => {
                let _res = self.decode_request(&buff, from, &mut state)?;
                todo!()
            }
            RESPONSE_ID => {
                let _res = decode_reply(&buff, from, &mut state)?;
                todo!()
            }
            _ => todo!("eror"),
        }
    }

    pub fn decode_request(self, buff: &[u8], mut from: Addr, state: &mut State) -> Result<Request> {
        let flags = buff[state.start()];
        state.add_start(1)?;

        let tid = state.decode_u16(buff)?;

        let to = Some(state.decode(buff)?);

        let id = decode_fixed_32_flag(flags, 1, state, buff)?;
        let token = decode_fixed_32_flag(flags, 2, state, buff)?;

        let internal = (flags & 4) != 0;
        let command: Command = state.decode(buff)?;

        let target = decode_fixed_32_flag(flags, 8, state, buff)?;

        let value = if flags & 16 > 0 {
            Some(state.decode_buffer(buff)?.as_ref().to_vec())
        } else {
            None
        };

        if let Some(id) = id {
            if let Some(valid_id) = validate_id(&id, &from) {
                from.id = valid_id.to_vec().into();
            }
        }

        Ok(Request {
            tid,
            from: Some(from),
            to,
            token,
            internal,
            command,
            target,
            value,
        })
    }
}

fn validate_id(id: &[u8; ID_SIZE], from: &Addr) -> Option<[u8; ID_SIZE]> {
    if let Ok(result) = calculate_id(from) {
        if *id == result {
            return Some(*id);
        }
    }
    None
}

fn decode_fixed_32_flag(
    flags: u8,
    shift: u8,
    state: &mut State,
    buff: &[u8],
) -> Result<Option<[u8; 32]>> {
    if flags & shift > 0 {
        return Ok(Some(
            state.decode_fixed_32(buff)?.as_ref().try_into().unwrap(),
        ));
    }
    return Ok(None);
}
/// Decode an u32 array
pub fn decode_addr_array(state: &mut State, buffer: &[u8]) -> Result<Vec<Addr>> {
    let len = state.decode_usize_var(buffer)?;
    let mut value: Vec<Addr> = Vec::with_capacity(len);
    for _ in 0..len {
        let add: Addr = state.decode(buffer)?;
        value.push(add);
    }
    Ok(value)
}

fn decode_reply(buff: &[u8], mut from: Addr, state: &mut State) -> Result<Reply> {
    let flags = buff[state.start()];
    state.add_start(1)?;

    let tid = state.decode_u16(buff)?;
    let to: Addr = state.decode(buff)?;

    let id = decode_fixed_32_flag(flags, 1, state, buff)?;
    let token = decode_fixed_32_flag(flags, 2, state, buff)?;

    let closer_nodes: Option<Vec<Addr>> = if flags & 4 > 0 {
        Some(decode_addr_array(state, buff)?)
    } else {
        None
    };

    let error: u8 = if flags & 8 > 0 {
        state.decode_u8(buff)?
    } else {
        0
    };

    let value = if flags & 16 > 0 {
        Some(state.decode_buffer(buff)?.to_vec())
    } else {
        None
    };

    if let Some(id) = id {
        if let Some(valid_id) = validate_id(&id, &from) {
            from.id = valid_id.to_vec().into();
        }
    }
    Ok(Reply {
        tid,
        rtt: 0,
        from,
        to,
        token,
        closer_nodes,
        error,
        value,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use async_udx::UdxSocket;
    use rand::{
        rngs::{OsRng, StdRng},
        RngCore, SeedableRng,
    };
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        time::Duration,
    };

    const HOST: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
    const BOOTSTRAP_PORT: u16 = 10001;
    const BOOTSTRAP_ADDR: Addr = Addr {
        id: None,
        host: HOST,
        port: BOOTSTRAP_PORT,
    };

    fn thirty_two_random_bytes() -> [u8; 32] {
        let mut buff = [0; 32];
        let mut rng = StdRng::from_rng(OsRng).unwrap();
        rng.fill_bytes(&mut buff);
        buff
    }

    /// This data and expected result are taken from the initial message a node sends to a bootstrap
    /// node in dht-rpc 6.15.1
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
                235, 159, 119, 93, 35, 250, 85, 76, 120, 152, 96, 17, 175, 157, 204, 216, 8, 191,
                189, 16, 140, 146, 202, 172, 84, 232, 73, 218, 113, 136, 161, 173,
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
        let sock = UdxSocket::bind("127.0.0.1:0").await?;
        let io = Io::default();
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
    async fn do_find_node() -> Result<()> {
        // data from find the first bootstrap call
        // the "target" is this.table.id for the bootstrap query
        let target = [
            119, 44, 181, 79, 186, 249, 110, 212, 6, 32, 70, 6, 149, 114, 119, 56, 168, 69, 210,
            251, 161, 149, 49, 223, 53, 187, 230, 101, 161, 56, 143, 163,
        ];
        //let value = None;
        // let command =  2 ;
        // let internal =  true ;
        // let token =  None ;
        // let to =  {
        //  id: <Buffer e9 6e 01 46 a3 3e 02 29 87 fe 25 6c 63 43 ac 5d 20 db a4 7e a1 b6 34 1f 93 29 03
        //b4 84 69 45 3e>,
        //  host: '127.0.0.1',
        //  port: 10001
        //} ;
        let sock = UdxSocket::bind("127.0.0.1:0").await?;
        let io = Io::default();
        // TODO finish find node
        let find_node_req = io.create_find_node(
            /// the node we are looking for?
            &BOOTSTRAP_ADDR,
            &target,
        );
        let buff = find_node_req.encode_request(&io, false)?;
        println!("{buff:?}");
        //find_node_req.en
        let sock_addr: SocketAddr = format!("{HOST}:{BOOTSTRAP_PORT}").parse().unwrap();
        sock.send(sock_addr, &buff);
        //socke.send(&B
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let res = sock.recv().await;
        dbg!(res);
        Ok(())
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
            id: None,
            host: HOST,
            port: BOOTSTRAP_PORT,
        };
        let io = Io::default();

        let res = req.encode_request(&io, false)?;
        assert_eq!(res, expected_buffer);
        Ok(())
    }

    #[test]
    fn test_decode_request() -> Result<()> {
        let state_start = 1;
        let state_end = 75;
        let buff = [
            3, 13, 38, 33, 127, 0, 0, 1, 17, 39, 186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115,
            50, 6, 25, 133, 59, 149, 198, 162, 26, 109, 183, 36, 71, 251, 134, 40, 25, 235, 205,
            135, 36, 2, 186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50, 6, 25, 133, 59, 149,
            198, 162, 26, 109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
        ];
        let from_before = Addr {
            id: None,
            host: HOST,
            port: 45475,
        };
        let value = None;
        let target = [
            186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50, 6, 25, 133, 59, 149, 198, 162,
            26, 109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
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
        let id = [
            186, 215, 155, 149, 209, 74, 57, 70, 15, 217, 115, 50, 6, 25, 133, 59, 149, 198, 162,
            26, 109, 183, 36, 71, 251, 134, 40, 25, 235, 205, 135, 36,
        ];

        let mut state = State::new_with_start_and_end(state_start, state_end);
        let io = Io::default();
        let res = io.decode_request(&buff, from.clone(), &mut state)?;
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
            19, 5, 73, 32, 127, 0, 0, 1, 193, 153, 233, 110, 1, 70, 163, 62, 2, 41, 135, 254, 37,
            108, 99, 67, 172, 93, 32, 219, 164, 126, 161, 182, 52, 31, 147, 41, 3, 180, 132, 105,
            69, 62, 1, 127, 0, 0, 1, 193, 153,
        ];
        let from = Addr {
            id: None,
            host: HOST,
            port: BOOTSTRAP_PORT,
        };
        let flags = 5;
        let tid = 8265;
        let to = Addr {
            id: None,
            host: HOST,
            port: 39361,
        };
        let id = [
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
        let validateId: Option<[u8; 32]> = Some([
            233, 110, 1, 70, 163, 62, 2, 41, 135, 254, 37, 108, 99, 67, 172, 93, 32, 219, 164, 126,
            161, 182, 52, 31, 147, 41, 3, 180, 132, 105, 69, 62,
        ]);

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
            error: error.into(),
            value,
        };
        assert_eq!(result, expected);
        Ok(())
    }
}
