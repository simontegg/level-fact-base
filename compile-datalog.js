const UUID = require('uuid/v4')
const { all, is, concat, contains, each, filter, find, groupBy, keys, sort, map, path, pipe, reduce, values } = require('rambda')
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

function clear (tuple) {
  return [
    tuple[0].replace('?', ''),
    tuple[1],
    tuple[2].replace('?', ''),
  ]
}

function getEntry (grouped, binding) {
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

function getJoins (grouped) {
  const joins = {}

  keys(grouped).forEach(entity1 => {
    keys(grouped).forEach(entity2 => {
      if (grouped[entity2].variables[entity1]) {
        joins[entity1] = entity2
      }
    })
  })

  return joins
}

function sortEntities (entry, grouped, joins) {
  const queries = []
  const done = {}

  function recurse (queries, entity) {
    queries.push(grouped[entity])
    done[entity] = true

    if (joins[entity] && !done[joins[entity]]) {
      return recurse(queries, joins[entity])
    }

    const reverseJoin = keys(joins).find(join => entity === joins[join] && !done[join])

    if (reverseJoin) {
      return recurse(queries, reverseJoin)
    }

    return queries
  }

  return recurse(queries, entry.entity)
}

function compileSubQueries (tuples, binding, select) { 
  const ts = map(clear, tuples)
  const grouped = group(ts)
  const joins = getJoins(grouped)
  const entry = getEntry(grouped, binding)
  const groups = sortEntities(entry, grouped, joins)
  const queries = collectQueries([], 0, false)


  function collectQueries (queries, i, join) {
    if (i === groups.length) {
      return queries
    }

    const { entity, variables } = groups[i]

    if (i === 0 && entry.attribute) {
      queries.push(entry)

      const byEntity = {
        entity,
        attributes: values(variables).filter(attr => attr !== entry.attribute)
      }

      queries.push(byEntity)
      const next = i + 1

      return collectQueries(queries, next, entity)
    }

    if (i === 0 && !entry.attribute) {
      const byEntity = { entity, attributes: values(variables) }
      queries.push(byEntity)

      return collectQueries(queries, i + 1, entity)
    }

    if (join) {
      const byJoin = { entity, attribute: variables[join], joinFrom: join }
      queries.push(byJoin)

      const vars = keys(variables)

      if (vars.length > 1) {
        const byEntity = { entity, attributes: [], joinTo: {} }

        for (let i = 0; i < vars.length; i++) {
          const variable = vars[i]

          if (variable !== join) {
            byEntity.attributes.push(variables[variable])

            if (joins[variable] === entity) {
              byEntity.joinTo[variables[variable]] = variable
            }
          }
        }

        queries.push(byEntity)
      }

      return collectQueries(queries, i + 1)
    }

    const byEntity = { entity, attributes: [], joinTo: {} }
    const vars = keys(variables)

    for (let i = 0; i < vars.length; i++) {
      const variable = vars[i]
      byEntity.attributes.push(variables[variable])

      if (joins[variable] === entity) {
        byEntity.joinTo[variables[variable]] = variable
      }
    }

    queries.push(byEntity)

    return collectQueries(queries, i + 1)
  }

  

  jsome(joins)
  jsome(groups)

  return queries
  





}
