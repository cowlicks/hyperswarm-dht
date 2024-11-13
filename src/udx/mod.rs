//! udx/dht-rpc internnals
//!
#![allow(unreachable_code, dead_code)]
use std::convert::TryFrom;
use std::fmt::Display;
use std::future::IntoFuture;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::{udx::io::Io, Result};
use rand::{
    rngs::{OsRng, StdRng},
    RngCore, SeedableRng,
};

mod cenc;
mod io;

#[cfg(test)]
mod test;

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

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Command::*;
        write!(f, "COMMAND")?;
        match self {
            Ping => write!(f, "::Ping"),
            PingNat => write!(f, "::PingNat"),
            FindNode => write!(f, "::FindNode"),
            DownHint => write!(f, "::DownHint"),
        }
    }
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

impl From<&Addr> for SocketAddr {
    fn from(value: &Addr) -> Self {
        SocketAddr::V4(SocketAddrV4::new(value.host, value.port))
    }
}

impl From<&SocketAddr> for Addr {
    fn from(value: &SocketAddr) -> Self {
        let SocketAddr::V4(s) = value else { panic!() };
        Addr {
            id: None,
            host: *s.ip(),
            port: s.port(),
        }
    }
}

#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct RpcDht {
    #[builder(default = "thirty_two_random_bytes()")]
    id: [u8; 32],
    #[builder(field(ty = "Vec<SocketAddr>"))]
    bootstrap_nodes: Vec<SocketAddr>,
    #[builder(default = "Self::default_io()?")]
    io: Io,
}

impl RpcDhtBuilder {
    fn add_bootstrap_node(
        mut self,
        addr: SocketAddr,
    ) -> std::result::Result<Self, RpcDhtBuilderError> {
        self.bootstrap_nodes.push(addr);
        Ok(self)
    }
    fn default_io() -> std::result::Result<Io, String> {
        Io::new().map_err(|e| e.to_string())
    }
}

impl RpcDht {
    async fn bootstrap(&self) -> Result<()> {
        let Some(node_addr) = self.bootstrap_nodes.last() else {
            panic!()
        };
        let to = Addr::from(node_addr);
        let r = self.io.send_find_node(&to, &self.id).await?;
        Ok(())
    }

    fn query(&mut self, _command: Command, _id: [u8; 32], _value: Option<Vec<u8>>) {
        todo!()
    }
}

fn thirty_two_random_bytes() -> [u8; 32] {
    let mut buff = [0; 32];
    let mut rng = StdRng::from_rng(OsRng).unwrap();
    rng.fill_bytes(&mut buff);
    buff
}
