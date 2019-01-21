

const redis = require('redis')
const factory = require('./index.js')
const UUID = require('uuid/v4')
const jsome = require('jsome')
const superagent = require('superagent')
const absolute =require('superagent-absolute')

const agent = superagent.agent()
const request = absolute(agent)('http://localhost:9200')




const client = require('./elastic.js')

const cache = redis.createClient('redis://127.0.0.1:6379')

const db = factory(client, cache)

const getId = () => UUID().replace(/-/g, '')

// const questionId = getId()
const questionId = '1'

const entities = [
  {
    $e: questionId,
    question_identifier: 'staffing'
  },
  {
    $e: 'a',
    answer_question_id: questionId,
    answer_name: 'thing',
    answer_comment: 'best'
  },
  {
    $e: 'b',
    answer_question_id: '2',
    answer_name: 'thing',
    answer_comment: 'worst'
  },
  {
    $e: 'c',
    answer_question_id: '3',
    answer_name: 'sysy',
    answer_comment: 'best'
  },
  {
    $e: 'd',
    answer_question_id: questionId,
    answer_name: 'thin',
    answer_comment: 'ccc',
    answer_section: 'ddd'
  }
]

const query = `
  SELECT *
    FROM facts AS f
   WHERE f.attribute = 'answer_question_id'
     AND f.value = ${questionId}
`

db.transact(entities, async err => {

  try {
    const res = await request
      .post('/_xpack/sql?format=json')
      .send({ query })

    // jsome(res)
    
    jsome(JSON.parse(res.text))
  } catch (err) {
    console.log({err});
  }


    



  await client.indices.delete({ index: '_all' })








})


// db.transact(entities, err => {
  // console.log({err});
//
// })

