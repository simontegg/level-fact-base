const pull = require('pull-stream')
const UUID = require('uuid/v4')
const { all, is, concat, contains, each, filter, find, keys, sort, map, path, pipe, reduce, values } = require('rambda')
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
  const indexMap      = {}

  const tupleMap = new Map()

  pipe(
    map((tuple, i) => {
      const entity    = tuple[0].replace('?', '')
      const attribute = tuple[1]
      const variable  = tuple[2].replace('?', '')
      const timestamp = is(String, tuple[3]) ? tuple[3].replace('?', '') : tuple[3]

      const eKey = `e|${entity}`
      const aKey = `a|${attribute}`
      const vKey = `v|${variable}`
      const tKey = `t|${timestamp}`

      if (indexMap[eKey] === undefined) {
        indexMap[eKey] = []
      }

      indexMap[eKey].push(i)
      indexMap[aKey] = i
      indexMap[vKey] = i
      indexMap[tKey] = i

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
      let joins = []
      
      const evKey  = `e|${variable}`
      const veKey  = `v|${entity}`
      const tvKey  = `t|${variable}`
      const vtKey  = `v|${timestamp}`

      if (indexMap[evKey] !== undefined) {
        joins = joins.concat(indexMap[evKey])  
      }

      if (indexMap[veKey] !== undefined) {
        joins.push(indexMap[veKey])
      }

      if (indexMap[tvKey] !== undefined) {
        joins.push(indexMap[tvKey])
      }

      if (indexMap[vtKey] !== undefined) {
        joins.push(indexMap[tvKey])
      }
      
      const boundEntity = binding[entity] 
      const boundValue = binding[variable]
      const boundTimestamp = binding[timestamp]
      
      const e   = `e|${entity}`   
      const peers = filter(j => j !== i, indexMap[e])

      tupleMap.set(
        i, 
        { joins, peers, boundEntity, boundValue, boundTimestamp, i }
      )

      // tuple is a join -> separate prioritised attribute query
      if (entityMap[variable]) {
        const aKey = `a|${attribute}`

        queries[aKey] = {
          type: 'join', 
          entity,
          attribute,
          score: boundQueries[variable] ? 70 : 50, 
          join: variable,
          timestamp,
          dependsOn: `e|${variable}`
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
    }),
  )(tuples)

  let head
  for (let join of tupleMap.values()) {
    if (join.boundEntity || join.boundValue) {
      head = join
      continue
    }
  }

  let maxPeers = 0
  for (let join of tupleMap.values()) {
    if (join.peers.length > maxPeers) {
      head = join
      maxPeers = join.peers.length
      continue
    }
  }



  const qs = buildQueries(tupleMap, head, [])
  // if ()
//
  function buildQueries (tupleMap, node, queries, type) {
    const { i, peers, joins } = node
    let query
    let next

    console.log({node, type});

    if (node.boundValue) {
      query = {
        type: 'entities-by-attribute-value',
        attribute: tuples[i][1],
        value: node.boundValue
      }

      queries.push(query)

      // fetch additional attributes
      if (peers.length > 0) {
        next = tupleMap.get(peers[0])
        next.peers = filter(j => j !== i, next.peers)
        return buildQueries(tupleMap, next, queries, 'attributes-from-entities')
      }

      // fetch join
      if (joins.length > 0) {
        next = tupleMap.get(joins[0])
        next.joins = filter(j => j !== i, next.joins)
        return buildQueries(tupleMap, next, queries, 'entities-by-joined-value')
      }

      return queries
    }
    
    // if (node.boundEntity) {
      // query = {
        // type: 'attributes-by-entity',
        // entity: tuples[i][0],
        // attributes: map(index => tuples[index][1], concat([i], peers)),
        // match: node.boundEntity
      // }
//
      // peers.forEach(j => tupleMap.delete(j))
      // next = tupleMap.get(joins[0])
//
    // }
//
    if (type === 'entities-by-joined-value') {
      query = {
        type,
        entity: tuples[i][0],
        attribute: tuples[i][1],
        join: tuples[i][2]
      }

      queries.push(query)

      if (peers.length > 0) {
        next = tupleMap.get(peers[0])
        next.peers = filter(j => j !== i, next.peers)
        return buildQueries(tupleMap, next, queries, 'attributes-from-entities')
      }

      return queries
    }
    
    if (type === 'attributes-from-entities') {
      query = { 
        type,
        entity: tuples[i][0], 
        attributes: map(index => tuples[index][1], concat([i], peers))
      }
      
      queries.push(query)
      // peers.forEach(j => tupleMap.delete(j))

      if (joins.length > 0) {
        next = tupleMap.get(joins[0])
        next.joins = []
        return buildQueries(tupleMap, next, queries, 'entities-by-joined-value')
      }
    }

    return queries
  }



  jsome(qs)
  console.log(head)
  tupleMap.forEach((val, i) => {
    console.log(i);
    jsome(val)
  })


  return [sortQueries(queries), attrMap]
}

const sortQueries = pipe(values, sort((a, b) => b.score - a.score))
