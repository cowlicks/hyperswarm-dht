use dht_rpc::PeerId;

pub struct QueryOpts {
    pub clear: bool,
    pub closest_nodes: Vec<PeerId>,
    pub only_closest_nodes: bool,
}
