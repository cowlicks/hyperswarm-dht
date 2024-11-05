const dht = require('dht-rpc')
const crypto = require('crypto')


function sha256 (val) {
  return crypto.createHash('sha256').update(val).digest()
}
// Set ephemeral: true as we are not part of the network.
const node = dht({
  ephemeral: true,
  bootstrap: [
      'localhost:10001'
    ]
})

const val = Buffer.from('foo');
console.log(sha256(val).toString('hex'));

node.update('values', sha256(val), val, function (err, res) {
  if (err) throw err
  console.log('Inserted', sha256(val).toString('hex'))
  after(sha256(val).toString('hex'))
})

function after(hexFromAbove) {
  node.query('values', Buffer.from(hexFromAbove, 'hex'))
  .on('data', function (data) {
    if (data.value && sha256(data.value).toString('hex') === hexFromAbove) {
      // We found the value! Destroy the query stream as there is no need to continue.
      console.log(val, '-->', data.value.toString())
      this.destroy()
    }
  })
  .on('end', function () {
    console.log('(query finished)')
  })
}
