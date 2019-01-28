const { all, append, is, concat, contains, each, equals, filter, find, groupBy, keys, sort, map, path, pipe, reduce, values } = require('rambda')
const getFilter = require('./get-filter')

const noop = () => {}

module.exports = function compileToElastic (q) {
  const filter = getFilter()
  let fromPrior


  // value search
  if (q.condition && !is(Object, q.condition) && q.attribute) {
    filter.must({ term: { 'attribute.keyword': q.attribute }})
    filter.must({ term: { 'value.keyword': q.condition }})
  }

  // entity search
  if (q.condition && q.attributes) {
    filter.must({ term: { entity: q.condition } })
    q.attributes.forEach(attr => filter.should({ term: { 'attribute.keyword': attr } }))
  }

  if (q.joinFrom) {
    filter.must({ term: { 'attribute.keyword': q.attribute } })

    // value from resultsMap
    fromPrior = (resultsMap, filter) => {
      keys(resultsMap[q.joinFrom] || {})
        .forEach(join => filter.should({ term: { 'value.keyword': join } }))
    }
  }

  if (!q.joinFrom && !q.condition) {
    fromPrior = (resultsMap, filter) => {
      keys(resultsMap[q.entity] || {})
        .forEach(entity => filter.should({ term: { entity } }))
    }
  }

  // general existing entity attributes search
  if (!q.condition && q.attributes) {
    q.attributes.forEach(attr => filter.should({ term: { 'attribute.keyword': attr } }))
  }

  q.filter = filter
  q.fromPrior = fromPrior

  return q
}
