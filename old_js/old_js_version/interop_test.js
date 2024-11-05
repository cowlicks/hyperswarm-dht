// make a BS node
// make another node connected to BS (node a)
// node A  announce
// run rust connect to BS node, do lookup
const dht = require('dht-rpc')

// Set ephemeral: true so other peers do not add us to the peer list, simply bootstrap
const bootstrap = dht({ ephemeral: true })

bootstrap.listen(10001)
