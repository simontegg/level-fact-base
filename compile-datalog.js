const UUID = require('uuid/v4')
const { all, append, is, concat, contains, each, equals, filter, find, groupBy, keys, sort, map, path, pipe, reduce, values } = require('rambda')
const Big = require('big.js')
const jsome = require('jsome')

const traverse = require('traverse')

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

function clear (tuple) {
  return [
    tuple[0].replace('?', ''),
    tuple[1],
    tuple[2].replace('?', ''),
  ]
}

function getEntry (grouped, binding) {
  let maxVars = 0
  let entry

  keys(grouped).some(entity => {
    if (binding[entity]) {
      entry = { entity, condition: binding[entity] }
      return true
    }

    return keys(binding).some(key => {
      if (grouped[entity].variables[key]) {
        entry = { attribute: grouped[entity].variables[key], entity, condition: binding[key] }
        return true
      }
    })

    // no binding -> entry is entity with most variables
    const varsCount = keys(grouped[entity].variables).length

    if (varsCount > maxVar) {
      maxVars = varsCount
      entry = { entity }
    }
  })

  return entry
}

function group (tuples) {
  const grouped = {}

  for (let i = 0; i < tuples.length; i++) {
    const [entity, attribute, variable] = tuples[i]
    if (!grouped[entity]) {
      grouped[entity] = { entity, tuples: [], variables: {} }
    }
      
    grouped[entity].tuples.push(tuples[i])
    grouped[entity].variables[tuples[i][2]] = tuples[i][1]
  }

  return grouped
}

function joinOrder (grouped, start) {
  const memo = []
  const ordered = [[start, []]]
  const done = {}
  const entities = keys(grouped).sort((a, b) => a === start ? -1 : 1)

  while (entities.length > 0) {
    const from = entities.shift()

    // iterate throught entities
    keys(grouped).forEach(entity => {

      // ignore own variables
      if (entity !== from) {

        // iterate through an entity's variables
        keys(grouped[entity].variables).forEach(variable => {

          // variable is an entity
          if (!done[entity] && from === variable) {
            traverse(ordered).forEach(function (x) {
              // prior entity exists in ordered results
              if (x === from) {
                done[x] = true
                // find path for subordinate entities
                let path = this.path.slice(0)
                path.splice(this.path.length - 1, 1, '1')

                const subordinates = traverse(ordered).get(path)
                const update = subordinates.slice(0)
                update.push([entity, []])
                traverse(ordered).set(path, update)
              }
            })
          }

          // 
          if (!done[variable] && grouped[variable]) {
            traverse(ordered).forEach(function (x) {
              if (!done[x] && x === entity) {
                done[variable] = true
                done[x] = true

                let path = this.path.slice(0)
                path.splice(this.path.length - 1, 1, '1')

                const subordinates = traverse(ordered).get(path)
                const update = subordinates.slice(0)
                update.push([variable, []])
                traverse(ordered).set(path, update)
              }
            })
          }
        })
      }
    })

    entities.sort((a, b) => done[a] ? -1 : 1)
  }

  return ordered
}

function compileSubQueries (tuples, binding, select) { 
  const ts = map(clear, tuples)
  const grouped = group(ts)
  const entry = getEntry(grouped, binding)
  const ordered = joinOrder(grouped, entry.entity)
  const boundVariables = keys(binding)
  jsome(ordered)

  const queries = collect(ordered, 0, [])
  const done = {}

  function collect(group, i, queries, parent) {
    const [entity, subordinates] = group[i]
    const { variables } = grouped[entity]

    for (let i = 0; i < boundVariables.length; i++) {
      const boundVariable = boundVariables[i]

      if (variables[boundVariable]) {
        const byValue = {
          entity, 
          attribute: variables[boundVariable], 
          condition: binding[boundVariable] 
        }
        
        // perform separate searches for each attribute:value combination
        // limit subsequent queries by existing entities in resultsMap
        queries.push(byValue)
      }
    }

    if (binding[entity]) {
      const byEntity = { entity, attributes: values(variables), condition: binding[entity] }
      queries.push(byEntity)
    }

    // JOIN FROM prior entity
    if (variables[parent]) {
      const byJoin = { entity, attribute: variables[parent], joinFrom: parent }
      const vars = keys(variables)
      queries.push(byJoin)

      if (vars.length > 1) {
        const byEntity = { entity, joinTo: {}, attributes: [] }
        const children = map(g => g[0], subordinates)

        for (let i = 0; i < vars.length; i++) {
          const variable = vars[i]

          if (variable !== parent) {
            byEntity.attributes.push(variables[variable])

            // variable matches a subordinate entity -> JOIN TO
            if (contains(variable, children)) {
              if (!byEntity.joinTo) {
                byEntity.joinTo = {}
              }

              byEntity.joinTo[variables[variable]] = variable
            }
          }
        }

        queries.push(byEntity)
      }
    }

    // GENERAL search from prior entity
    if (!variables[parent] && entity !== entry.entity) {
      const byEntity = { entity, attributes: [] }
      const vars = keys(variables)
      const children = map(g => g[0], subordinates)

      for (let i = 0; i < vars.length; i++) {
        const variable = vars[i]

        if (variable !== parent) {
          byEntity.attributes.push(variables[variable])

          // variable matches a subordinate entity -> JOIN TO
          if (contains(variable, children)) {
            if (!byEntity.joinTo) {
              byEntity.joinTo = {}
            }

            byEntity.joinTo[variables[variable]] = variable
          }
        }
      }

      queries.push(byEntity)
    }

    // recurse
    for (let j = 0; j < subordinates.length; j++) {
      collect(subordinates, j, queries, entity)
    }

    return queries
  }

  return queries
}
