const dht = require('dht-rpc')

// Set ephemeral: true so other peers do not add us to the peer list, simply bootstrap
const bootstrap = dht({ ephemeral: true })

bootstrap.listen(10001)
console.log('listened');
setInterval(() => console.log(bootstrap.nodes), 3*1000)
