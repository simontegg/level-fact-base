const elastic = require('elasticsearch')

var client = new elastic.Client({
  host: 'localhost:9200',
  log: 'error'
})

module.exports = client


