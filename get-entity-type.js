const { keys, find, pipe, split, head } = require('rambda')

const getEntityType = pipe(
  keys,
  find(key => key !== '$e' && key !== '$retract' && key !== 'create'),
  split('_'),
  head
)

module.exports = getEntityType

        
