const { all, append, is, concat, contains, each, equals, filter, find, groupBy, keys, sort, map, path, pipe, reduce, values } = require('rambda')
const getFilter = require('./get-filter')
const jsome = require('jsome')

const noop = () => {}

module.exports = function compileToElastic (q) {
  const filter = getFilter()
  let fromPrior


  // value search
  if (q.condition && !is(Object, q.condition) && q.attribute) {
    filter.must({ term: { 'attribute.keyword': q.attribute }})
    filter.must({ term: { 'value.keyword': q.condition }})

    // AND logic from prior conditions 
    fromPrior = (resultsMap, filter) => {
      keys(resultsMap[q.entity] || {})
        .forEach(prior => filter.should({ term: { entity: prior } }))
    }
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

  // general existing entity attributes search
  if (!q.joinFrom && !q.condition) {
    const attributeCondition = { bool: { should: [] } }
    q.attributes.forEach(attr => {
      attributeCondition.bool.should.push({ term: { 'attribute.keyword': attr } })
    })

    filter.must(attributeCondition)

    fromPrior = (resultsMap, filter) => {
      const entityIds = keys(resultsMap[q.entity] || {})
      
      if (entityIds.length > 0) {
        const entityCondition = { bool: { should: [] } }
        entityIds.forEach(entity => entityCondition.bool.should.push({ term: { entity } }))
        filter.must(entityCondition)
      }
    }
  }

  if (q.atTimestamp) {
    const timestamps = keys(q.atTimestamp)
    console.log(q.atTimestamp);
    jsome(filter.build());

    for (let i = 0; i < timestamps.length; i++) {

    }


  }

  q.filter = filter
  q.fromPrior = fromPrior

  return q
}
