use crate::{crypto::Keypair2, HyperDhtEvent, Result};

use compact_encoding::types::CompactEncodable;
use dht_rpc::{IdBytes, Response};
use tracing::instrument;

#[derive(Debug)]
pub struct LookupResponse {
    pub response: Response,
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
            response: resp.clone(),
            peers,
        }))
    }
}

impl From<LookupResponse> for HyperDhtEvent {
    fn from(value: LookupResponse) -> Self {
        HyperDhtEvent::LookupResponse(value)
    }
}

#[derive(Debug)]
pub struct AnnounceInner {
    pub topic: IdBytes,
    pub responses: Vec<Response>,
    pub keypair: Keypair2,
}

impl AnnounceInner {
    /// Store the decoded peers from the `Response` value
    #[instrument(skip_all)]
    pub fn inject_response(&mut self, resp: Response) {
        self.responses.push(resp);
    }
}

// this should store info about the unannounce requests and
#[derive(Debug)]
pub struct UnannounceInner {
    pub topic: IdBytes,
    pub responses: Vec<Response>,
    pub keypair: Keypair2,
}

impl UnannounceInner {
    /// Store the decoded peers from the `Response` value
    #[instrument(skip_all)]
    pub fn inject_response(&mut self, resp: Response, _tid: u16) {
        self.responses.push(resp);
    }
}

#[derive(Debug)]
pub struct UnannounceResult {}
