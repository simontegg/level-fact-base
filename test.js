const jsome = require('jsome')
const compileDatalog = require('./compile-datalog')
const test = require('ava')

test('one-by-value-to-many', t => {
  const tuples = [
    ['?orgId', 'org_name', '?orgName'],
    ['?orgId', 'org_status', '?status'], 
    ['?orgId', 'org_updatedAt', '?orgUpdatedAt'], 

    ['?relId', 'relationship_objectId', '?orgId'],
    ['?relId', 'relationship_subjectId', '?id'],

    ['?id', 'person_name', '?name'],
    ['?id', 'person_updatedAt', '?updatedAt']
  ]
  const binding = { orgName: 'denmark' }
  const select = ['name', 'updatedAt', 'status', 'orgUpdatedAt']

  const result = compileDatalog(tuples, binding, select)

  console.log('queries');
  jsome(result)

  
  t.is(true, true)

})

test('one-by-id-to-many', t => {
  const tuples = [
    ['?orgId', 'org_name', '?orgName'],
    ['?orgId', 'org_status', '?status'], 
    ['?orgId', 'org_updatedAt', '?orgUpdatedAt'], 

    ['?relId', 'relationship_objectId', '?orgId'],
    ['?relId', 'relationship_subjectId', '?id'],

    ['?id', 'person_name', '?name'],
    ['?id', 'person_updatedAt', '?updatedAt']
  ]

  const binding = { orgId: 'd' }
  const select = ['name', 'updatedAt', 'status', 'orgUpdatedAt']

  const result = compileDatalog(tuples, binding, select)

  console.log('queries');
  jsome(result)

  
  t.is(true, true)

})


test('one-by-id-to-multplie-many', t => {
  const tuples = [
    ['?orgId', 'org_name', '?orgName'],
    ['?orgId', 'org_status', '?status'], 
    ['?orgId', 'org_updatedAt', '?orgUpdatedAt'], 

    ['?relId', 'relationship_objectId', '?orgId'],
    ['?relId', 'relationship_subjectId', '?id'],

    ['?olId', 'orgLocality_orgId', '?orgId'],
    ['?olId', 'orgLocality_localityId', '?localityId'],

    ['?id', 'person_name', '?name'],
    ['?id', 'person_updatedAt', '?updatedAt']
  ]

  const binding = { orgId: 'd' }
  const select = ['name', 'updatedAt', 'status', 'orgUpdatedAt']

  const result = compileDatalog(tuples, binding, select)

  console.log('queries');
  jsome(result)

  
  t.is(true, true)
})

test('multiple condtions', t => {
  const tuples = [
    ['?orgId', 'org_name', '?orgName'],
    ['?orgId', 'org_status', '?status'], 
    ['?orgId', 'org_updatedAt', '?orgUpdatedAt'], 

    ['?relId', 'relationship_objectId', '?orgId'],
    ['?relId', 'relationship_subjectId', '?id'],

    ['?olId', 'orgLocality_orgId', '?orgId'],
    ['?olId', 'orgLocality_localityId', '?localityId'],

    ['?id', 'person_name', '?name'],
    ['?id', 'person_updatedAt', '?updatedAt']
  ]

  const binding = { orgName: 'denmark', status: 'good' }
  const select = ['name', 'updatedAt', 'status', 'orgUpdatedAt']

  const result = compileDatalog(tuples, binding, select)

  console.log('queries');
  jsome(result)

  
  t.is(true, true)
})

test.only('cross-bound-timestamps', t => {
  const tuples = [
    ['?orgId',  'org_name',         '?orgName'],
    ['?orgId',  'org_status',       '?status'],
    ['?orgId',  'org_updatedAt',    '?orgUpdatedAt'], 

    ['?id',     'person_name',      '?name',        '?orgUpdatedAt'],
    ['?id',     'person_updatedAt', '?updatedAt',   '?orgUpdatedAt']
  ]

  const binding = { orgName: 'denmark' }
  const select = ['name', 'updatedAt', 'status', 'orgUpdatedAt']

  const result = compileDatalog(tuples, binding, select)

  jsome(result)

  
  t.is(true, true)
})

test('bound-timestamps', t => {
  const tuples = [
    ['?id',     'person_name',      '?name',        '?past'],
    ['?id',     'person_updatedAt', '?updatedAt',   '?past']
  ]

  const binding = { name: 'denmark', past: 12344949995 }
  const select = ['name', 'updatedAt']

  const result = compileDatalog(tuples, binding, select)

  jsome(result)

  
  t.is(true, true)
})

