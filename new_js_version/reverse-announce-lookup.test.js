#!/usr/bin/env node
const dht = require('hyperdht');

// in order to bootstrap we start an
// ephemeral node with empty bootstrap array
// and then call listen on it
const PORT = 10021;
const HOST = '127.0.0.1';
const bs = dht.bootstrapper(PORT, HOST);
console.log(`${HOST}:${PORT}`);

bs.on('ready', (...x) => {
  console.log('ready', ...x);
});
bs.on('connection', (...x) => {
  console.log('connection', ...x);
});
bs.on('open', (...x) => {
  console.log('connection', ...x);
});
bs.on('initial-nodes', (...x) => {
  console.log('initial-nodes', ...x);
});

bs.on('listening', (...x) => {
  console.log('listening', ...x);
});

bs.on('close', (...x) => {
  console.log('close', ...x);
});

bs.on('holepunch', (...x) => {
  console.log('holepunch', ...x);
});

bs.on('data', (...x) => {
  console.log('data', ...x);
});
bs.on('end', (...x) => {
  console.log('end', ...x);
});


setInterval(() => {
  console.log(bs.nodes);
}, 1000*5);


/*
bs.listen(function () {
    const { address, port } = bs.address()
    console.log(`${address}:${port}`)
})
*/
