use std::{collections::BTreeMap, sync::Arc};

use crate::{
    commands,
    crypto::{namespace, Keypair2},
    request_announce_or_unannounce_value, HyperDhtEvent, Result,
};

use compact_encoding::types::CompactEncodable;
use dht_rpc::{
    io::InResponse, query::QueryId, Command, ExternalCommand, IdBytes, Peer, PeerId,
    RequestMsgData, Tid,
};
use tracing::instrument;

#[derive(Debug)]
pub struct LookupResponse {
    pub response: Arc<InResponse>,
    pub peers: Vec<crate::cenc::Peer>,
}

impl LookupResponse {
    /// `Ok(None)` when lookup response is missing value field.
    pub fn from_response(resp: Arc<InResponse>) -> Result<Option<Self>> {
        let Some(value) = &resp.response.value else {
            return Ok(None);
        };
        let (peers, _rest): (Vec<crate::cenc::Peer>, &[u8]) =
            <Vec<crate::cenc::Peer> as CompactEncodable>::decode(value)?;
        Ok(Some(LookupResponse {
            response: resp,
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
    pub responses: Vec<Arc<InResponse>>,
    pub keypair: Keypair2,
}

impl AnnounceInner {
    pub fn new(topic: IdBytes, keypair: Keypair2) -> Self {
        Self {
            topic,
            keypair,
            responses: Default::default(),
        }
    }
    /// Store the decoded peers from the `Response` value
    #[instrument(skip_all)]
    pub fn inject_response(&mut self, resp: Arc<InResponse>) {
        self.responses.push(resp);
    }
}

// this should store info about the unannounce requests and
#[derive(Debug)]
#[allow(unused)]
pub struct UnannounceInner {
    pub topic: IdBytes,
    pub responses: Vec<Arc<InResponse>>,
    pub inflight_unannounces: BTreeMap<Tid, (Arc<InResponse>, UnannounceRequest)>,
    pub keypair: Keypair2,
}

impl UnannounceInner {
    pub fn new(topic: IdBytes, keypair: Keypair2) -> Self {
        Self {
            topic,
            keypair,
            responses: Default::default(),
            inflight_unannounces: Default::default(),
        }
    }
    /// Store the decoded peers from the `Response` value
    #[instrument(skip_all)]
    pub fn inject_response(&mut self, resp: Arc<InResponse>, _tid: u16) {
        self.responses.push(resp);
    }
}

#[derive(Debug)]
pub struct UnannounceResult {}

#[derive(Debug)]
pub struct UnannounceRequest {
    pub tid: Tid,
    pub query_id: QueryId,
    pub keypair: Keypair2,
    pub topic: IdBytes,
    pub token: [u8; 32],
    pub destination: PeerId,
}

impl From<UnannounceRequest> for RequestMsgData {
    fn from(
        UnannounceRequest {
            tid,
            query_id,
            keypair,
            topic,
            token,
            destination,
        }: UnannounceRequest,
    ) -> Self {
        let value = request_announce_or_unannounce_value(
            &keypair,
            topic,
            &token,
            destination.id,
            &[],
            &namespace::UNANNOUNCE,
        );

        let destination = Peer {
            id: Some(destination.id.0),
            addr: destination.addr,
            referrer: None,
        };
        RequestMsgData {
            tid,
            to: destination,
            //id: todo!(),
            // TODO... should this be in UnannounceRequest? Maybe it should just be added by
            // rpc::IoHandler
            id: None,
            token: Some(token),
            command: Command::External(ExternalCommand(commands::UNANNOUNCE)),
            target: Some(topic.0),
            value: Some(value),
        }
    }
}
