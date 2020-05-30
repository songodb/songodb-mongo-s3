require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {
  findMany,
  findOne
} = require('../lib/find')
const { insertMany } = require('../lib/insert')
const { deleteAll } = require('../lib/delete')

describe('findMany', () => {
  let instance = "test"
  let db = 'find'
  let collection = 'findMany'
  let prefix = `${instance}/${db}/${collection}/`
  beforeEach(async () => {
    await insertMany(s3, prefix, [ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } ])
  })
  afterEach(async () => {
    await deleteAll(s3, prefix)
  })
  it ('should find using simple query', async () => {
    let result = await findMany(s3, prefix, { first: "Jane" })
    expect(result).toEqual({
      docs: [ { _id: "1", first: "Jane", last: "Doe" } ],
      explain: {
        executionStats: {
          nReturned: 1,
          executionTimeMillis: expect.anything(),
          totalKeysExamined: 0,
          totalDocsExamined: 3,
        },
        s3: {
          IsTruncated: false,
          KeyCount: 3,
          MaxKeys: 100,
          NextContinuationToken: null,
          TimeMillis: expect.anything()
        }
      }
    })
  })
  it ('should get a single record by id and skip full scan', async () => {
    let result = await findMany(s3, prefix, { _id: "2" })
    expect(result).toEqual({
      "docs": [
        {
          "_id": "2",
          "first": "John",
          "last": "Doe"
        }
      ],
      "explain": {
        "executionStats": {
          "nReturned": 1,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 1
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 1,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything()
        }
      }
    })
  })
  it ('should return empty array if no docs found', async () => {
    let result = await findMany(s3, prefix, { _id: "BAD" })
    expect(result).toEqual({
      "docs": [],
      "explain": {
        "executionStats": {
          "nReturned": 0,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 0
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 0,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything()
        }
      }
    })
  })
  it ('should find all if null query given', async () => {
    let result = await findMany(s3, prefix, null)
    expect(result.docs.length).toBe(3)
    expect(result.docs.map(doc => doc["_id"]).sort()).toEqual([ "1", "2", "3" ])
  })
  it ('should find all if empty query given', async () => {
    let result = await findMany(s3, prefix, { })
    expect(result.docs.length).toBe(3)
    expect(result.docs.map(doc => doc["_id"]).sort()).toEqual([ "1", "2", "3" ])
  })
  it ('should sort by a single field', async () => {
    let result = await findMany(s3, prefix, null, { sort: [ [ "first", 1 ] ] })
    expect(result.docs.map(doc => doc.first)).toEqual([ "Jane", "Joe", "John" ])
  })
  it ('should skip and limit', async () => {
    let result = await findMany(s3, prefix, null, { 
      skip: 1, 
      limit: 1, 
      sort: [ [ "first", 1 ] ] 
    })
    expect(result.docs.length).toBe(1)
    expect(result.docs[0].first).toEqual("Joe")
  })
  it ('should not throw if db or collection does not exist i.e. bad prefix', async () => {
    let result = await findMany(s3, 'bad/prefix/', { last: "Smith" })
    expect(result).toMatchObject({
      docs: [  ]
    })
  })
  it ('should support projection operation', async () => {
    let result = await findMany(s3, prefix, { first: "Jane" }, { projection: { first: 1 } })
    expect(result.docs).toEqual([ { _id: "1", first: "Jane" } ])
  })
})

describe('findOne', () => {
  let instance = "test"
  let db = 'find'
  let collection = 'findOne'
  let prefix = `${instance}/${db}/${collection}/`
  beforeEach(async () => {
    await insertMany(s3, prefix, [ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } ])
  })
  afterEach(async () => {
    await deleteAll(s3, prefix)
  })
  it ('should find one doc when multiple match', async () => {
    let result = await findOne(s3, prefix, { last: "Doe" })
    expect(result).toEqual({
      docs: [ { _id: "1", first: "Jane", last: "Doe" } ],
      explain: {
        executionStats: {
          nReturned: 1,
          executionTimeMillis: expect.anything(),
          totalKeysExamined: 0,
          totalDocsExamined: 3,
        },
        s3: {
          IsTruncated: false,
          KeyCount: 3,
          MaxKeys: 100,
          NextContinuationToken: null,
          TimeMillis: expect.anything()
        }
      }
    })
  })
  it ('should return empty docs if none match', async () => {
    let result = await findOne(s3, prefix, { last: "Smith" })
    expect(result).toMatchObject({
      docs: [  ]
    })
  })
})