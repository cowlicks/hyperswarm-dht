//const DHT = require('dht-rpc');
import DHT from './dht-rpc/index.js'

const opts = { ephemeral: false, host: '127.0.0.1', }

const node = DHT.bootstrapper(10001, '127.0.0.1', {...opts})
await node.fullyBootstrapped();
console.log('boooootd');

setInterval(() => console.log(node.nodes), 5*1000);
