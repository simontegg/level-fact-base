const test = require('ava')
const redis = require('redis')
const UUID = require('uuid/v4')
const jsome = require('jsome')

const factory = require('./index.js')
const client = require('./elastic.js')
const cache = redis.createClient('redis://127.0.0.1:6379')

const db = factory(client, cache)

const getId = () => UUID().replace(/-/g, '')

// const questionId = getId()
const questionId = '1'

const entities = [
  {
    $e: 'a',
    person_name: 'alice',
  },
  {
    $e: 'b',
    person_name: 'bob',
  },
  {
    $e: 'c',
    person_name: 'charles',
  },
  {
    $e: 'd',
    org_name: 'denmark',
    org_status: 'good'
  },
  {
    $e: 'e',
    org_name: 'estonia'
  },
  {
    $e: 'e',
    org_name: 'finland'
  },
  { 
    $e: getId(),
    relationship_type: 'member_of',
    relationship_subjectId: 'a',
    relationship_objectId: 'd',
  },
  { 
    $e: 'rel2',
    relationship_type: 'member_of',
    relationship_subjectId: 'b',
    relationship_objectId: 'd',
  },
  { 
    $e: getId(),
    relationship_type: 'member_of',
    relationship_subjectId: 'c',
    relationship_objectId: 'e',
  }
]

const update = [
  {
    $e: 'a',
    person_name: 'alicia'
  },
  {
    $e: 'rel2',
    relationship_type: 'member_of',
    relationship_subjectId: 'b',
    relationship_objectId: 'd',
    $retract: true
  },
  {
    $e: 'd',
    org_status: 'bad'
  }
]

    // <, >, <=, >=, =, !=
    // count
    // ||
          // ['?name'  '>=' '?n'],
          // ['?name'   '==' ['denmark' '||' 'estonia']]

test.cb('many-to-many', t => {
  db.transact(entities, err => {
    const past = new Date().getTime() - 1

    db.transact(update, err => {
      console.log({err});
      db.query(
        [
          ['?orgId', 'org_name', '?orgName'],
          ['?orgId', 'org_status', '?status'], 
          ['?orgId', 'org_updatedAt', '?orgUpdatedAt'], 

          ['?relId', 'relationship_objectId', '?orgId'],
          ['?relId', 'relationship_subjectId', '?id']

          // ['?id', 'person_name', '?name'],
          // ['?id', 'person_updatedAt', '?updatedAt']
        ],
        { orgName: 'denmark' },
        ['status', 'relId'],
        (err, results) => {
          console.log({err});
          console.log('results');
          jsome(results)
    //
          client.msearch({ body: [{}, { query: { match_all: {} } }] }, (err, all) => {
            console.log({err});

            // jsome(all)
            client.indices.delete({ index: '_all' }, err => {
              console.log({err});

              client.indices.clearCache({ index: '_all' }, err => {
                console.log({err});

                t.end()
              })
            })
          })
        }
      )
    })
  })
})
