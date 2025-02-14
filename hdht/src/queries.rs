use crate::{
    commands,
    crypto::{namespace, Keypair2},
    request_announce_or_unannounce_value, HyperDhtEvent, Result,
};

use compact_encoding::types::CompactEncodable;
use dht_rpc::{
    query::QueryId, Command, ExternalCommand, IdBytes, Peer, PeerId, RequestMsgData, Response, Tid,
};
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
