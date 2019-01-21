const pull = require('pull-stream')
const UUID = require('uuid/v4')
const bodybuilder = require('bodybuilder')
const { all, is, contains, each, filter, keys, sort, map, path, pipe, values } = require('rambda')
const Big = require('big.js')
const jsome = require('jsome')

const compileSubQueries = require('./compile-subqueries')
const getFilter = require('./get-filter')

function getMonotonticTimestamp (cache, callback) {
  // handles 10,000 unique transaction ids per millisecond
  const one = Big(1)
  const tenK = Big(10000)

  cache.xadd('x', '*', 'y', 'z', (err, streamId) => {
    if (err) {
      return callback(err)
    }

    const parts = streamId.split('-')
    const epoch = parseInt(parts[0])
    const sequence = parseFloat(one.minus(tenK.minus(parseInt(parts[1])).div(10000)))
    callback(null, epoch + sequence)
  })
}

const getSorted = pipe(values, sort((a, b) => b.score - a.score))
const getHits = path('hits.hits')
const allRemoved = pipe(values, all(v => v === undefined))

module.exports = function (client, cache) {
  return {
    transact: function (entities, callback) {
      // get stateful transaction id
      getMonotonticTimestamp(cache, (err, timestamp) => {
        if (err) {
          return callback(err)
        }

        const facts = []

        for (let i = 0; i < entities.length; i++) {
          const entity = entities[i]
          
          keys(entity).forEach(attr => {
            if (attr === '$e' || attr === '$retract') {
              return
            }

            facts.push(
              { index: { _index: 'facts', _type: "_doc", _id: UUID() } }
            )
            
            facts.push({ 
              entity: entity.$e,
              attribute: attr, 
              value: entity[attr], 
              timestamp, 
              operation: !entity.$retract 
            })
          })
        }

        client.bulk({ body: facts, refresh: 'true' }, callback)
      })
    },

    _subQuery: function (query, resultsMap, callback) {
      const { entity, type, join, timestamp } = query
      const latestResults = {}
      const filter = getFilter()

      if (!resultsMap[entity]) {
        resultsMap[entity] = {}
      }

      // fetch entities that match attribute and value
      if (type === 'value') {
        filter.must({ term: { attribute: query.attribute }})
        filter.must({ term: { value: query.match }})
      }

      if (type === 'join') {
        filter.must({ term: { attribute: query.attribute }})

        const joinIds = keys(resultsMap[join] || {})
        
        if (joinIds.length > 0) {
          joinIds.forEach(joinId => {
            filter.should({ term: { value: joinId }})
          })
        } else {
          const entityIds = keys(resultsMap[entity] || {})

          entityIds.forEach(entityId => {
            filter.should({ term: { entity: entityId }})
          })
        }

      }

      if (type === 'attribute') {
        const entityIds = keys(resultsMap[entity] || {})
        const attributes = keys(query.attributes)

        if (is(Number, timestamp)) {
          filter.lte(timestamp)
        }
        
        // match facts with ANY 
        if (entityIds.length === 0) {
          attributes.forEach(attribute => filter.should({ term: { attribute } }))
        } else {
          const entityCondition = { bool: { should: [] } }
          const attrCondition   = { bool: { should: [] } }

          entityIds.forEach(entityId => {
            entityCondition.bool.should.push({ term: { entity: entityId } })
          })
          
          attributes.forEach(attribute => {
            attrCondition.bool.should.push({ term: { attribute } })
          })
          
          // facts must match ANY of entities AND ANY of attributes
          filter.must(entityCondition)
          filter.must(attrCondition)
        }
      }

      return pull(
        pull.once(filter.build()),
        pull.map(b => {
          jsome(b)
          return b
        }),
        pull.asyncMap((body, cb) => client.search({ index: 'facts', body }, cb)),
        pull.map(getHits),
        pull.flatten(),
        pull.map(hit => hit._source),
        pull.filter(fact => {
          const key = `${fact.entity}|${fact.attribute}`

          if (!latestResults[key]) {
            latestResults[key] = fact.timestamp
            return true
          }

          if (fact.timestamp > latestResults[key]) {
            latestResults[key] = fact.timestamp
            return true
          }

          return false
        }),
        pull.map(fact => {
          jsome(fact)

          if (!resultsMap[entity][fact.entity]) {
            resultsMap[entity][fact.entity] = {}
          }
          
          resultsMap[entity][fact.entity][fact.attribute] = fact.operation ? fact.value : undefined
          // clean up retracted entities
          if (fact.operation === false && allRemoved(resultsMap[entity][fact.entity])) {
            delete resultsMap[entity][fact.entity]
          }

          if (fact.entity === 'rel2') {
            console.log('LOGG');
            jsome(resultsMap)
          }

          if (join) {
            // add value to resultsMap for pickup
            if (!resultsMap[join]) {
              resultsMap[join] = {}
            }
            
            if (!resultsMap[join][fact.value]) {
              resultsMap[join][fact.value] = {}
            }

            if (!resultsMap[join][fact.value][fact.attribute]) {
              resultsMap[join][fact.value][fact.attribute] = {}
            }

            resultsMap[join][fact.value][fact.attribute][fact.entity] = true
          }
          
          return fact
        }),
        pull.collect(callback)
      )
    },

    query: function (tuples, binding, select, callback) {
      const [queries, attrMap] = compileSubQueries(tuples, binding, select)

      jsome(queries)
      const resultsMap = {}

      return pull(
        pull.values(queries),
        pull.asyncMap((query, cb) => this._subQuery(query, resultsMap, cb)),
        pull.collect((err, results) => {
          if (err) {
            return callback(err)
          }

          // jsome(queries)

          callback(null, processResults(resultsMap, attrMap))
        })
      )
    }
  }
}



function processResults (resultsMap, attrMap) {
  const results = []

  keys(resultsMap).forEach(entityType => {
    keys(resultsMap[entityType]).forEach(entityId => {
      const entity = { [entityType]: entityId }

      keys(resultsMap[entityType][entityId]).forEach(attr => {
        const select = attrMap[attr]

        if (select) {
          entity[select] = resultsMap[entityType][entityId][attr]
        }
      })

      results.push(entity)
    })
  })

  return results
}



