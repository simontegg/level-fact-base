const pull = require('pull-stream')
const UUID = require('uuid/v4')
const bodybuilder = require('bodybuilder')
const { contains, each, keys, sort, map, path, pipe, values } = require('rambda')
const Big = require('big.js')
const jsome = require('jsome')

function escapeVar (elm) {
  return typeof elm === 'string'
    ? elm
      .replace(/^\\/, '\\\\')
      .replace(/^\?/, '\\?')
    : elm
}

function isVar (val) {
  return /^\?/.test(val)
}

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

function getEntityMap (tuples, binding, select) {
  const entityMap = {}

  for (let i = 0; i < tuples.length + 1; i++) {
    const tuple = tuples[i]
    let score = 0
    let typeId
    let attr

    for (let j = 0; j < tuple.length; j++) {
      const variable = tuple[j].replace('?', '')

      if (j === 0) {
        typeId = variable
      }

      if (j === 1) {
        attr = variable
      }

      if (!entityMap[typeId]) {
        entityMap[typeId] = { typeId, binding: {}, as: {}, score: 0 }
      }

      if (binding[variable]) {
        entityMap[typeId].binding[j === 0 ? typeId : attr] = binding[variable]
        entityMap[typeId].score += variable === typeId ? 2 : 1
      }

      if (j === 2 && contains(variable, select)) {
        entityMap[typeId].as[attr] = variable
      }
    }
  }

  return entityMap
}




const getSorted = pipe(values, sort((a, b) => b.score - a.score))
const getHits = path('hits.hits')

function getFilter () {
  const body = { query: { bool: { filter: { bool: { must: [], should: [] } } } } }

  return {

    must: function (condition) {
      body.query.bool.filter.bool.must.push(condition)
    },

    should: function (condition) {
      body.query.bool.filter.bool.should.push(condition)
    },

    build: function () {
      return body
    }
  }
}

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

    _queryTuple: function (resultsMap, tuple, binding, callback) {
      const typeId  = tuple[0].replace('?', '')
      const attr    = tuple[1]
      const select  = tuple[2].replace('?', '')

      if (!resultsMap[typeId]) {
        resultsMap[typeId] = {}
      }

      // if (!resultsMap[attr]) {
        // resultsMap[attr] = {}
      // }

      // const body = bodybuilder()
      // const body = { query: { bool: { must: [], should: [] } } }
      const filter = getFilter()
      const entityIds = keys(resultsMap[typeId])
      const value = binding[select]
      const joins = resultsMap[select] ? keys(resultsMap[select]) : []
      
      // restrict to existing results entities ({ attr: 'val1' AND attr2: 'val2' })
      if (entityIds.length > 0) {
        entityIds.forEach(entityId => {
          filter.should({ term: { entity: entityId } })
        })
      }

      filter.must({ term: { attribute: attr }})
      // query for matching bound value
      if (value) {
        filter.must({ term: { value } })
      }

      // query for joined entity
      if (joins.length > 0) {
        joins.forEach(entityId => {
          filter.should({ term: { value: entityId }})
        })
      }

      const built = filter.build()

      console.log('TYPEIDS');
      jsome(entityIds)
      console.log('BBB');
      jsome(built)

      return pull(
        pull.once(built),
        pull.asyncMap((body, cb) => client.search({ index: 'facts', body }, cb)),
        pull.map(getHits),
        pull.flatten(),
        pull.map(hit => hit._source),
        pull.map(fact => {
          if (!resultsMap[typeId][fact.entity]) {
            resultsMap[typeId][fact.entity] = {}
          }

          resultsMap[typeId][fact.entity][fact.attribute] = fact.value
          // resultsMap[fact.attribute][fact.entity] = fact.value
          return fact
        }),
        pull.collect(callback)
      )
    },

    query: function (tuples, binding, select, callback) {
      // const entityMap = getEntityMap(tuples, binding, select)
      const sorted = scoreAndSort(tuples, binding)
      const unbound = []

      const resultsMap = {}
      
      console.log('SORTED');
      jsome(sorted)

      return pull(
        pull.values(sorted),
        // pull.filter(({ tuple, score }) => {
          // if (score === 0) {
            // unbound.push(tuple)
            // return false
          // }
//
          // return true
        // }),
        pull.asyncMap(({ tuple }, cb) => this._queryTuple(resultsMap, tuple, binding, cb)),
        pull.collect((err, results) => {
          if (err) {
            return callback(err)
          }

          callback(null, [resultsMap, results, unbound])
        })
      )
    }
  }
}

function scoreAndSort (tuples, binding) {
  const scoreMap = {}
  const run = pipe(
    map((tuple, i) => {
      scoreMap[i] = 0

      for (let j = 0; j < tuples.length + 1; j++) {
        const variable = tuple[j].replace('?', '')

        if (binding[variable]) {
          scoreMap[i] += j === 0 ? 2 : 1
        }
      }

      return tuple
    }),
    map((tuple, i) => ({ tuple, score: scoreMap[i] })),
    sort((a, b) => b.score - a.score)
  )

  return run(tuples)
}


function processResults (resulltsMap, entityMap) {
  const results = []

  // keys(resulltsMap).forEach( => {
//
  // })
  

}
