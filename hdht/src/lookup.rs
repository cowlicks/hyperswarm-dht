use crate::{HyperDhtEvent, Result};
use std::net::SocketAddr;

use compact_encoding::types::CompactEncodable;
use dht_rpc::Response;

#[derive(Debug)]
pub struct LookupResponse {
    _token: Option<[u8; 32]>,
    _from: SocketAddr,
    _to: Option<SocketAddr>,
    pub peers: Vec<crate::cenc::Peer>,
}

impl LookupResponse {
    /// `Ok(None)` when lookup response is missing value field.
    pub fn from_response(resp: &Response) -> Result<Option<Self>> {
        let Some(value) = &resp.value else {
            return Ok(None);
        };
        let (peers, _rest): (Vec<crate::cenc::Peer>, &[u8]) =
            <Vec<crate::cenc::Peer> as CompactEncodable>::decode(value)?;
        Ok(Some(LookupResponse {
            _token: resp.token,
            _from: resp.peer,
            _to: resp.to,
            peers,
        }))
    }
}

impl From<LookupResponse> for HyperDhtEvent {
    fn from(value: LookupResponse) -> Self {
        HyperDhtEvent::LookupResponse(value)
    }
}
