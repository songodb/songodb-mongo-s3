require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {
  updateMany,
  updateOne,
  replaceOne
} = require('../lib/update')
const { findMany, findOne } = require('../lib/find')
const { insertMany } = require('../lib/insert')
const { deleteAll } = require('../lib/delete')

describe('updateMany', () => {
  let instance = "test"
  let db = 'update'
  let collection = 'updateMany'
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
  it ('should set a simple field on a single found doc', async () => {
    let result = await updateMany(s3, prefix, { first: "Jane" }, { "$set": { last: "Goodall" } })
    expect(result).toEqual({
      "matchedCount": 1,
      "modifiedCount": 1,
      "upsertedCount": 0,
      "upsertedId": null,
      "explain": {
        "executionStats": {
          "nReturned": 1,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 3
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 3,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything()
        }
      }
    })
    let { docs } = await findMany(s3, prefix, { first: "Jane" })
    expect(docs).toEqual([
      { _id: "1", first: "Jane", last: "Goodall" }
    ])
  })
  it ('should update and write multiple docs', async () => {
    let result = await updateMany(s3, prefix, { last: "Doe" }, { "$set": { last: "Smith" } })
    expect(result).toMatchObject({
      "matchedCount": 3,
      "modifiedCount": 3,
      "upsertedCount": 0,
      "upsertedId": null
    })
    let { docs } = await findMany(s3, prefix, { last: "Smith" }, { sort: [ [ "_id", 1 ] ]})
    expect(docs).toEqual([
      { _id: "1", first: "Jane", last: "Smith" }, 
      { _id: "2", first: "John", last: "Smith" },
      { _id: "3", first: "Joe", last: "Smith" } 
    ])
  })
  it ('should only write modified docs', async () => {
    let result = await updateMany(s3, prefix, { last: "Doe" }, { "$set": { first: "Jane" } })
    expect(result).toMatchObject({
      "matchedCount": 3,
      "modifiedCount": 2,
      "upsertedCount": 0,
      "upsertedId": null
    })
  })
  it ('should upsert if no docs match and upsert option is true', async () => {
    let result = await updateMany(s3, prefix, 
      { first: "Jill" }, 
      { "$set": { first: "Jill", last: "Doe" } },
      { upsert: true }
    )
    expect(result).toMatchObject({
      "matchedCount": 0,
      "modifiedCount": 0,
      "upsertedCount": 1,
      "upsertedId": {
        "_id": expect.anything()
      }
    })
    let { docs } = await findMany(s3, prefix, { first: "Jill" })
    expect(docs).toEqual([
      { _id: expect.anything(), first: "Jill", last: "Doe" } 
    ])
  })
  it ('should do nothing if no docs match', async () => {
    let result = await updateMany(s3, prefix, 
      { first: "Jill" }, 
      { "$set": { first: "Jill", last: "Doe" } })
    expect(result).toMatchObject({
      "matchedCount": 0,
      "modifiedCount": 0,
      "upsertedCount": 0,
      "upsertedId": null
    })
  })
})

describe('updateOne', () => {
  let instance = "test"
  let db = 'update'
  let collection = 'updateMany'
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
  it ('should update only one doc even if multiple match', async () => {
    let result = await updateOne(s3, prefix, { last: "Doe" }, { "$set": { last: "Smith" } })
    expect(result).toEqual({
      "matchedCount": 1,
      "modifiedCount": 1,
      "upsertedCount": 0,
      "upsertedId": null,
      "explain": {
        "executionStats": {
          "nReturned": 1,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 3
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 3,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything()
        }
      }
    })
    let { docs } = await findOne(s3, prefix, { last: "Smith" })
    expect(docs).toEqual([ { "_id": "1", "first": "Jane", "last": "Smith" } ])
  })
  it ('should return nModified 0 if the one doc matched is not affected by update', async () => {
    let result = await updateOne(s3, prefix, { last: "Doe" }, { "$set": { last: "Doe" } })
    expect(result).toMatchObject({
      "matchedCount": 1,
      "modifiedCount": 0,
      "upsertedCount": 0,
      "upsertedId": null,
    })
  })
})

describe('replaceOne', () => {
  let instance = "test"
  let db = 'update'
  let collection = 'replaceOne'
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
  it ('should replace a doc', async () => {
    let result = await replaceOne(s3, prefix, { first: "Jane" }, { first: "Jill", last: "Doe" })
    expect(result).toEqual({
      "matchedCount": 1,
      "modifiedCount": 1,
      "upsertedCount": 0,
      "upsertedId": null,
      "explain": {
        "executionStats": {
          "nReturned": 1,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 3
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 3,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything()
        }
      }
    })
    let { docs } = await findMany(s3, prefix, { _id: "1" })
    expect(docs).toEqual([ { "_id": "1", first: "Jill", last: "Doe" } ])
  })
  it ('should upsert a doc if upsert option is given', async () => {
    let result = await replaceOne(s3, prefix, 
      { first: "Jill" }, 
      { first: "Jill", last: "Doe" },
      { upsert: true })
    expect(result).toMatchObject({
      "matchedCount": 0,
      "modifiedCount": 0,
      "upsertedCount": 1,
      "upsertedId": { _id: expect.anything() }
    })
    let { docs } = await findMany(s3, prefix, { first: "Jill" })
    expect(docs).toEqual([ { "_id": expect.anything(), first: "Jill", last: "Doe" } ])
  })
})


