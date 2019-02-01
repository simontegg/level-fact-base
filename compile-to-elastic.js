const { all, append, is, concat, contains, each, equals, filter, find, groupBy, keys, sort, map, path, pipe, reduce, values } = require('rambda')
const getFilter = require('./get-filter')

const noop = () => {}

module.exports = function compileToElastic (q) {
  const filter = getFilter()
  let fromPrior
  let runAfterEach

  // value search
  if (q.condition && !is(Object, q.condition) && q.attribute) {
    filter.must({ term: { 'attribute.keyword': q.attribute }})
    filter.must({ term: { 'value.keyword': q.condition }})
  }

  if (q.conditions) {

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

  q.filter = filter
  q.fromPrior = fromPrior

  return q
}
