const client = require('./elastic')

function flush (callback) {
  client.indices.delete({ index: '_all' }, err => {
    client.indices.clearCache({ index: '_all' }, callback)
  })
}

flush(err => {
  console.log({err});
})


