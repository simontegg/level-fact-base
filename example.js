

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
    $e: questionId,
    question_identifier: 'staffing',
    question_ui: 'ui-1'
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


db.transact(entities, err => {
  console.log({err});
  db.query(
    [
      ['?questionId', 'question_identifier', '?identifier'],
      ['?questionId', 'question_ui', '?ui'],
      //
      ['?id', 'answer_question_id', '?questionId'],

      ['?id', 'answer_name', '?name'],
      ['?id', 'answer_comment', '?comment']
    ],
    { questionId },
    // ['id', 'comment'],
    ['comment', 'name', 'ui'],
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
        })

      })





    }
  )


})


// db.transact(entities, err => {
  // console.log({err});
//
// })

