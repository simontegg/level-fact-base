const pull = require('pull-stream')
const UUID = require('uuid/v4')
const { is, contains, each, filter, keys, sort, map, path, pipe, values } = require('rambda')
const Big = require('big.js')
const jsome = require('jsome')

module.exports = compileSubQueries

function getTimestamp (timestamp, binding) {
  if (!timestamp) {
    return null
  }

  if (binding[timestamp]) {
    return binding[timestamp]
  }

  if (is(Number, timestamp)) {
    return timestamp
  }

  // TODO timestamp variable joins to other tuple
}

function compileSubQueries (tuples, binding, select) {
  const entityMap     = {}
  const varMap        = {}
  const attrMap       = {}
  const queries       = {}
  const boundQueries  = {}

  pipe(
    map((tuple, i) => {
      const entity    = tuple[0].replace('?', '')
      const attribute = tuple[1]
      const variable  = tuple[2].replace('?', '')
      const timestamp = is(String, tuple[3]) ? tuple[3].replace('?', '') : tuple[3]

      // keep maps of entities, variables and select
      if (!entityMap[entity]) {
        entityMap[entity] = {}
      }

      entityMap[entity][attribute] = variable

      if (!varMap[variable]) {
        varMap[variable] = {}
      }

      varMap[variable][attribute] = entity

      if (contains(variable, select)) {
        attrMap[attribute] = variable
      }

      // fetch facts where entity matches binding (and associated attributes)
      if (binding[entity]) {
        const key = `e|${entity}`
        boundQueries[entity] = true

        if (!queries[key]) {
          queries[key] = { 
            type: 'entity', 
            entity, 
            match: binding[entity], 
            attributes: {},
            score: 200,
            timestamp: getTimestamp(timestamp, binding)
          }
        }
      }

      // fetch facts where value and attribute match binding
      if (typeof binding[variable] !== 'undefined') {
        boundQueries[entity] = true

        queries[`v|${attribute}`] = { 
          type: 'value', 
          entity, 
          attribute, 
          match: binding[variable], 
          variable,
          score: 100,
          timestamp: getTimestamp(timestamp, binding)
        }
      }

      return [entity, attribute, variable, timestamp]
    }),
    map((tuple, i) => {
      const [entity, attribute, variable, timestamp] = tuple

      // tuple is a join -> separate prioritised attribute query
      if (entityMap[variable]) {
        const aKey = `a|${attribute}`

        queries[aKey] = {
          type: 'join', 
          entity,
          attribute,
          score: boundQueries[variable] ? 70 : 50, 
          join: variable
        }

        return tuple
      }

      // timestamp depends on prior result
      // if (varMap[timestamp]) {
        // const aKey = `j|${attribute}`
//
        // queries[aKey] = {
          // type: 'join',
          // entity,
          // attribute,
          // score: boundQueries[variable] ? 70 : 50,
          // join: variable
        // }
//
        // return tuple
      // }


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
          queries[aKey] = { type: 'attribute', attributes: {}, entity, score: 0, timestamp }
        }

        queries[aKey].attributes[attribute] = variable
      }
      
      return tuple
    })
  )(tuples)

  console.log('varMap');
  jsome(varMap)

  return [sortQueries(queries), attrMap]
}

const sortQueries = pipe(values, sort((a, b) => b.score - a.score))
