const pull = require('pull-stream')
const UUID = require('uuid/v4')
const bodybuilder = require('bodybuilder')
const { contains, each, filter, keys, sort, map, path, pipe, values } = require('rambda')
const Big = require('big.js')
const jsome = require('jsome')

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

    _queryTuple: function (query, binding, resultsMap, callback) {
      const { entity, type } = query

      if (!resultsMap[entity]) {
        resultsMap[entity] = {}
      }

      const filter = getFilter()

      // fetch entities that match attribute and value
      if (type === 'value') {
        filter.must({ term: { attribute: query.attribute }})
        filter.must({ term: { value: query.match }})
      }

      // fetch match
      if (type === 'attribute') {
        const entityIds = keys(resultsMap[entity])
        const attributes = keys(query.attributes)
        
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

      const built = filter.build()

      jsome(resultsMap)
      console.log('BBB');
      jsome(built)

      return pull(
        pull.once(built),
        pull.asyncMap((body, cb) => client.search({ index: 'facts', body }, cb)),
        pull.map(getHits),
        pull.flatten(),
        pull.map(hit => hit._source),
        pull.map(fact => {
          if (!resultsMap[entity][fact.entity]) {
            resultsMap[entity][fact.entity] = {}
          }
          
          resultsMap[entity][fact.entity][fact.attribute] = fact.value

          const joinVar = query.joins[fact.attribute]

          if (joinVar) {
            if (!resultsMap[joinVar]) {
              resultsMap[joinVar] = {}
            }
            
            if (!resultsMap[joinVar][fact.value]) {
              resultsMap[joinVar][fact.value] = {}
            }

            resultsMap[joinVar][fact.value][fact.attribute] = fact.entity
          }
          
          jsome(fact)
          return fact
        }),
        pull.collect(callback)
      )
    },

    query: function (tuples, binding, select, callback) {
      const queries = compileSubQueries(tuples, binding)
      jsome(queries)
      const resultsMap = {}

      return pull(
        pull.values(queries),
        pull.asyncMap((query, cb) => this._queryTuple(query, binding, resultsMap, cb)),
        pull.collect((err, results) => {
          if (err) {
            return callback(err)
          }

          jsome(queries)

          callback(null, results)

          // callback(null, processResults(resultsMap, attrMap))
        })
      )
    }
  }
}

function compileSubQueries (tuples, binding) {
  const entityMap = {}
  const varMap = {}
  const queries = {}

  pipe(
    map((tuple, i) => {
      const entity = tuple[0].replace('?', '')
      const attribute = tuple[1]
      const variable = tuple[2].replace('?', '')

      if (!entityMap[entity]) {
        entityMap[entity] = {}
      }

      entityMap[entity][attribute] = variable

      if (!varMap[variable]) {
        varMap[variable] = {}
      }

      varMap[variable][attribute] = entity
      
      // fetch facts where entity matches binding (and associated attributes)
      if (binding[entity]) {
        const key = `e|${entity}`
        if (!queries[key]) {
          queries[key] = { 
            type: 'entity', 
            entity, 
            match: binding[entity], 
            attributes: {},
            score: 200,
            joins: {}
          }
        }
      }

      // fetch facts where value and attribute match binding
      if (typeof binding[variable] !== 'undefined') {
        queries[`v|${attribute}`] = { 
          type: 'value', 
          entity, 
          attribute, 
          match: binding[variable], 
          variable,
          score: 100,
          joins: {}
        }
      }

      return [entity, attribute, variable]
    }),
    map((tuple, i) => {
      const [entity, attribute, variable] = tuple
      const eKey = `e|${entity}`

      // add unbound attributes to head query
      if (queries[eKey] && entityMap[entity][attribute]) {
        queries[eKey].attributes[attribute] = variable
        queries[eKey].score += 1
        
        if (entityMap[variable]) {
          queries[ekey].joins[attribute] = variable
        }

      } else {
        const aKey = `a|${entity}`

        if (!queries[aKey]) {
          queries[aKey] = { type: 'attribute', attributes: {}, entity, score: 0, joins: {} }
        }

        if (!queries[`v|${attribute}`]) {
          queries[aKey].attributes[attribute] = variable
        }

        // contains a join -> promote score
        if (entityMap[variable]) {
          queries[aKey].score += 1
          queries[aKey].joins[attribute] = variable
        }


      }
      
      return tuple
    })
  )(tuples)

  return sortQueries(queries)
}

const sortQueries = pipe(values, sort((a, b) => b.score - a.score))


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



