const { all, append, is, concat, contains, each, equals, filter, find, groupBy, keys, sort, map, path, pipe, reduce, values } = require('rambda')
const getFilter = require('./get-filter')
const jsome = require('jsome')

const noop = () => {}

module.exports = function compileToElastic (q) {
  const filter = getFilter()
  let fromPrior
  let getTimestamp

  if (is(Number, q.lte)) {
    filter.lte(q.lte)
  }

  if (is(Object, q.lte)) {
    getTimestamp = resultsMap => {
      const { entity, variable } = q.lte

      // only works if there is a single entity
      const entityId = keys(resultsMap[entity])[0]
      const date = resultsMap[entity][entityId][variable]
      return is(String, date) ? new Date(date).getTime() : date
    }
  }



  // value search
  if (q.condition && !is(Object, q.condition) && q.attribute) {
    filter.must({ term: { 'attribute.keyword': q.attribute }})
    filter.must({ term: { 'value.keyword': q.condition }})

    // AND logic from prior conditions 
    fromPrior = (resultsMap, filter) => {
      keys(resultsMap[q.entity] || {})
        .forEach(prior => filter.should({ term: { entity: prior } }))

      if (getTimestamp) {
        filter.lte(getTimestamp(resultsMap))
      }
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
      
      if (getTimestamp) {
        filter.lte(getTimestamp(resultsMap))
      }
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
      
      if (getTimestamp) {
        filter.lte(getTimestamp(resultsMap))
      }
    }
  }

  if (getTimestamp && !fromPrior) {
    fromPrior = (resultsMap, filter) => filter.lte(getTimestamp(resultsMap))
  }

  q.filter = filter
  q.fromPrior = fromPrior

  return q
}
