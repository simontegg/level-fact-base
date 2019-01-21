module.exports = getFilter

function getFilter () {
  const body = { 
    query: { 
      bool: { 
        filter: { 
          bool: { 
            must: [], 
            should: [] 
          } 
        } 
      }
    } 
  }

  return {

    must: function (condition) {
      body.query.bool.filter.bool.must.push(condition)
    },

    should: function (condition) {
      body.query.bool.filter.bool.should.push(condition)
    },

    sort: function (order) {
      if (!body.sort) {
        body.sort = []
      }
//
      body.sort.push(order)
    },

    lte: function (timestamp) {
      body.query.bool.filter.bool.must.push({ range: { timestamp: { lte: timestamp } } })
    },

    build: function () {
      return body
    }
  }
}
