use std::collections::BTreeMap;

use crate::{RequestMsgData, Tid};
use futures::channel::mpsc;
use wasm_timer::Instant;

const DEFAULT_EXTERNAL_REQUEST_CHANNEL_SIZE: usize = 64;

#[derive(Debug)]
pub struct ExternalRequests {
    reciever: mpsc::Receiver<RequestMsgData>,
    sender: mpsc::Sender<RequestMsgData>,
    inflight: BTreeMap<Tid, Instant>,
}

impl ExternalRequests {
    pub fn new() -> Self {
        let (sender, reciever) = mpsc::channel(DEFAULT_EXTERNAL_REQUEST_CHANNEL_SIZE);
        Self {
            reciever,
            sender,
            inflight: Default::default(),
        }
    }
    fn new_sender(&self) -> mpsc::Sender<RequestMsgData> {
        self.sender.clone()
    }
    pub fn remove_tid(&mut self, tid: &Tid) -> Option<Instant> {
        self.inflight.remove(tid)
    }
    pub fn insert_tid(&mut self, tid: Tid) -> Option<Instant> {
        self.inflight.insert(tid, Instant::now())
    }
    pub fn is_done(&self) -> bool {
        self.inflight.is_empty()
    }
    pub fn poll(&mut self) -> Option<Vec<RequestMsgData>> {
        let mut out = vec![];
        while let Some(e) = self.reciever.try_next().ok().flatten() {
            self.insert_tid(e.tid);
            out.push(e)
        }
        out.is_empty().then(|| out)
    }
}

impl Default for ExternalRequests {
    fn default() -> Self {
        Self::new()
    }
}
