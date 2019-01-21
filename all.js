const jsome = require('jsome')
const client = require('./elastic')

function all (callback) {
  client.search({ index: '_all', body: { query: { match_all: {} } } }, callback)
}

all((err, results) => {
  console.log({err});

  jsome(results)
})


