

const redis = require('redis')
const factory = require('./index.js')
const UUID = require('uuid/v4')
const jsome = require('jsome')


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
    org_name: 'denmark'
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
    relationship_subject_id: 'a',
    relationship_object_id: 'd',
  },
  { 
    $e: 'rel2',
    relationship_type: 'member_of',
    relationship_subject_id: 'b',
    relationship_object_id: 'd',
  },
  { 
    $e: getId(),
    relationship_type: 'member_of',
    relationship_subject_id: 'c',
    relationship_object_id: 'e',
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
    relationship_subject_id: 'b',
    relationship_object_id: 'd',
    $retract: true
  }
]


db.transact(entities, err => {
  const past = new Date().getTime() - 1

  db.transact(update, err => {
    console.log({err});
    db.query(
      [
        ['?orgId', 'org_name', '?orgName'],
        //
        ['?relId', 'relationship_subject_id', '?id'],
        ['?relId', 'relationship_object_id', '?orgId'],

        ['?id', 'person_name', '?name']
      ],
      { orgName: 'denmark' },
      // ['id', 'comment'],
      ['name'],
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
            })
          })

        })





      }
    )




})


  


})


// db.transact(entities, err => {
  // console.log({err});
//
// })

