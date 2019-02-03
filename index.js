const pull = require('pull-stream')
const UUID = require('uuid/v4')
const bodybuilder = require('bodybuilder')
const { all, is, contains, each, filter, keys, sort, map, path, pipe, values } = require('rambda')
const Big = require('big.js')
const jsome = require('jsome')

const compileToElastic = require('./compile-to-elastic')
const compileDatalog = require('./compile-datalog')

const getFilter = require('./get-filter')
const getEntityType = require('./get-entity-type')

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

        return pull(
          pull.values(entities),
          pull.map(entity => {
            if (entity.$seed) {
              delete entity.$seed
              return entity
            }

            const type = getEntityType(entity)
            const createdAt = `${type}_createdAt`
            const now = new Date(timestamp).toISOString()

            if (entity.create && !entity[createdAt]) {
              entity[createdAt] = new Date()
              delete entity.create
            }

            const updatedAt = `${type}_updatedAt`

            if (!entity[updatedAt]) {
              entity[updatedAt] = now
            }

            return entity
          }),
          pull.map(entity => {
            const facts = []  

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

            return facts
          }),
          pull.flatten(),
          pull.collect((err, facts) => {
            if (err) {
              return callback(err)
            }

            // jsome(facts)

            client.bulk({ body: facts, refresh: 'true' }, callback)
          })
        )

      })
    },

    _subQuery: function (query, resultsMap, callback) {
      const { entity, joinTo, filter } = query
      const latestResults = {}

      if (!resultsMap[entity]) {
        resultsMap[entity] = {}
      }

      if (query.fromPrior) {
        query.fromPrior(resultsMap, filter)
      }

      // if (is(Number, timestamp)) {
        // filter.lte(timestamp)
        // }
      //
      const body = filter.build()

      return pull(
        pull.once(body),
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
          if (!resultsMap[entity][fact.entity]) {
            resultsMap[entity][fact.entity] = {}
          }
          
          resultsMap[entity][fact.entity][fact.attribute] = fact.operation ? fact.value : undefined

          // clean up retracted entities
          if (fact.operation === false && allRemoved(resultsMap[entity][fact.entity])) {
            delete resultsMap[entity][fact.entity]
          }

          const joinTos = keys(joinTo || {})

          if (joinTos.length > 0) {
            // add entity value to resultsMap for pickup by subsequent queries
            for (let i = 0; i < joinTos.length; i++) {
              const joinAttr = joinTos[i]

              if (joinAttr === fact.attribute) {
                const join = joinTo[joinAttr]

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

            }
          }

          if (query.runAfterEach) {
            console.log('runAfterEach');
            query.runAfterEach(resultsMap)
          }
          
          return fact
        }),
        pull.collect(callback)
      )
    },

    query: function (tuples, binding, select, callback) {
      const queries = compileDatalog(tuples, binding)
      const resultsMap = {}

      jsome(queries)

      return pull(
        pull.values(queries),
        pull.map(compileToElastic),
        pull.asyncMap((query, cb) => this._subQuery(query, resultsMap, cb)),
        pull.collect((err, results) => {
          if (err) {
            return callback(err)
          }

          jsome(results)

          // callback(null, processResults(resultsMap, attrMap))
          callback(null, resultsMap)
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



