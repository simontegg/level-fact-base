const client = require('./elastic')

function flush (callback) {
  client.indices.delete({ index: '_all' }, callback)
}

module.exports = flush

