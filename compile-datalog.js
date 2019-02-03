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
    tuple[3] ? tuple[3].replace('?', '') : undefined
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
    const [entity, attribute, variable, timestamp] = tuples[i]
    if (!grouped[entity]) {
      grouped[entity] = { entity, tuples: [], variables: {} }
    }
      
    grouped[entity].tuples.push(tuples[i])
    grouped[entity].variables[variable] = attribute

    if (timestamp) {
      grouped[entity].timestamp = timestamp
    }
  }

  return grouped
}

function joinOrder (grouped, start) {
  const memo = []
  const ordered = [[start, []]]
  const done = {}
  const entities = keys(grouped).sort((a, b) => a === start ? -1 : 1)

  jsome(grouped)

  while (entities.length > 0) {
    const from = entities.shift()

    // iterate throught entities
    keys(grouped).forEach(entity => {

      // ignore own variables
      if (entity !== from) {

        const { timestamp } = grouped[entity]

        if (timestamp) {
          keys(grouped).forEach(e => {
            if (!done[entity] && e !== entity) {
              keys(grouped[e].variables).forEach(v => {
                if (timestamp === v) {
                  traverse(ordered).forEach(function (x) {
                    if (!done[x] && x == e) { 
                      done[entity] = true

                      let path = this.path.slice(0)
                      path.splice(this.path.length - 1, 1, '1')

                      const subordinates = traverse(ordered).get(path)
                      const update = subordinates.slice(0)

                      update.push([entity, []])
                      traverse(ordered).set(path, update)
                    }
                  })
                }
              })
            }
          })
        }

        // iterate through other entity's variables
        keys(grouped[entity].variables).forEach(variable => {

          // child variable == entity 
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

          // child variable is entity
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

  const done = {}
  const queries = collect(ordered, 0, [])

  function collect(group, i, queries, parent) {
    const [entity, subordinates] = group[i]
    const { variables, timestamp } = grouped[entity]
    const vars = keys(variables)
    let lte

    if (timestamp) {
      keys(grouped[parent].variables).some(v => {
        if (v === timestamp) {
          lte = { [parent]: grouped[parent].variables[v] }
          return true
        }
      })

      if (binding[timestamp]) {
        lte = binding[timestamp]
      }
    }
    
    // entity by id
    if (binding[entity]) {
      const byEntity = { entity, attributes: values(variables), condition: binding[entity], lte }
      queries.push(byEntity)
    }
    
    // JOIN FROM prior entity
    if (variables[parent]) {
      const attr = variables[parent]
      const byJoin = { entity, attribute: attr, joinFrom: parent, lte }
      queries.push(byJoin)
    }

    // entity by value
    for (let i = 0; i < boundVariables.length; i++) {
      const boundVariable = boundVariables[i]
      const attr = variables[boundVariable]

      if (attr) {
        const byValue = {
          entity, 
          attribute: attr, 
          condition: binding[boundVariable],
          lte
        }
        
        // perform separate searches for each attribute:value combination
        // limit subsequent queries by existing entities in resultsMap
        queries.push(byValue)
      }
    }

    // GENERAL search from prior entity
    if (!binding[entity] && vars.length > 0) {
      const byEntity = { entity, attributes: [], lte }
      const children = map(g => g[0], subordinates)

      for (let i = 0; i < vars.length; i++) {
        const variable = vars[i]

        if (variable !== parent && !binding[variable]) {
          const attr = variables[variable]
          byEntity.attributes.push(attr)

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
