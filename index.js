const pull = require('pull-stream')
const UUID = require('uuid/v4')
const bodybuilder = require('bodybuilder')
const { is, contains, each, filter, keys, sort, map, path, pipe, values } = require('rambda')
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

    _subQuery: function (query, resultsMap, callback) {
      const { entity, type, join } = query

      if (!resultsMap[entity]) {
        resultsMap[entity] = {}
      }

      const filter = getFilter()

      // fetch entities that match attribute and value
      if (type === 'value') {
        filter.must({ term: { attribute: query.attribute }})
        filter.must({ term: { value: query.match }})
      }

      if (type === 'join') {
        filter.must({ term: { attribute: query.attribute }})

        const joinIds = keys(resultsMap[join] || {})
        joinIds.forEach(joinId => {
          filter.should({ term: { value: joinId }})
        })
      }

      if (type === 'attribute') {
        const entityIds = keys(resultsMap[entity] || {})
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

      return pull(
        pull.once(filter.build()),
        pull.asyncMap((body, cb) => client.search({ index: 'facts', body }, cb)),
        pull.map(getHits),
        pull.flatten(),
        pull.map(hit => hit._source),
        pull.map(fact => {
          if (!resultsMap[entity][fact.entity]) {
            resultsMap[entity][fact.entity] = {}
          }
          
          resultsMap[entity][fact.entity][fact.attribute] = fact.value

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
      const resultsMap = {}

      return pull(
        pull.values(queries),
        pull.asyncMap((query, cb) => this._subQuery(query, resultsMap, cb)),
        pull.collect((err, results) => {
          if (err) {
            return callback(err)
          }

          jsome(queries)

          callback(null, processResults(resultsMap, attrMap))
        })
      )
    }
  }
}

function compileSubQueries (tuples, binding, select) {
  const entityMap = {}
  const attrMap = {}
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

      if (contains(variable, select)) {
        attrMap[attribute] = variable
      }
//
      // varMap[variable][attribute] = entity
      
      // fetch facts where entity matches binding (and associated attributes)
      if (binding[entity]) {
        const key = `e|${entity}`
        if (!queries[key]) {
          queries[key] = { 
            type: 'entity', 
            entity, 
            match: binding[entity], 
            attributes: {},
            score: 200
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
          score: 100
        }
      }

      return [entity, attribute, variable]
    }),
    map((tuple, i) => {
      const [entity, attribute, variable] = tuple

      // tuple is a join -> separate prioritised attribute query
      if (entityMap[variable]) {
        const aKey = `a|${attribute}`
        queries[aKey] = { 
          type: 'join', 
          entity,
          attribute,
          score: 50, 
          join: variable
        }

        return tuple
      }

      // add unbound attributes to any entity query
      const eKey = `e|${entity}`

      if (queries[eKey] && entityMap[entity][attribute]) {
        queries[eKey].attributes[attribute] = variable
        queries[eKey].score += 1

        // otherwise place in a general attributes query
      } else {
        if (queries[`v|${attribute}`]) {
          return
        }

        const aKey = `a|${entity}`

        if (!queries[aKey]) {
          queries[aKey] = { type: 'attribute', attributes: {}, entity, score: 0 }
        }

        queries[aKey].attributes[attribute] = variable
      }
      
      return tuple
    })
  )(tuples)

  return [sortQueries(queries), attrMap]
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



