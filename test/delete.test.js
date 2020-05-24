require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {
  deleteMany,
  deleteOne,
  deleteAll,
  deleteById
} = require('../lib/delete')
const { insertMany } = require('../lib/insert')

describe('deleteMany', () => {
  let instance = 'test'
  let db = 'delete'
  let collection = 'deleteMany'
  let prefix = `${instance}/${db}/${collection}/`
  beforeEach(async () => {
    await insertMany(s3, prefix, [ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" } ])
  })
  afterEach(async () => {
    await deleteAll(s3, prefix)
  })
  it ('should delete multiple docs', async () => {
    let filter = { last: "Doe" }
    let result = await deleteMany(s3, prefix, filter)
    expect(result).toEqual({
      "deletedCount": 2,
      "explain": {
        "executionStats": {
          "nReturned": 2,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 2
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 2,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything(),
          "Deleted": [ 
            { "Key": "test/delete/deleteMany/1" },
            { "Key": "test/delete/deleteMany/2" }
          ],
          "Errors": []
        }
      }
    })
  })
  it ('should shortcut when deleting just using _id', async () => {
    let filter = { _id: "2" }
    let result = await deleteMany(s3, prefix, filter)
    expect(result).toEqual({
      "deletedCount": 1,
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
          "MaxKeys": null,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything(),
          "Deleted": [ 
            { "Key": "test/delete/deleteMany/2" }
          ],
          "Errors": []
        }
      }
    })
  })
  it ('should not throw if prefix does not exist', async () => {
    let filter = { last: "Smith" }
    let result = await deleteMany(s3, prefix, filter)
    expect(result).toMatchObject({ "deletedCount": 0 })
  })
})

describe('deleteOne', () => {
  let instance = 'test'
  let db = 'delete'
  let collection = 'deleteOne'
  let prefix = `${instance}/${db}/${collection}/`
  beforeEach(async () => {
    await insertMany(s3, prefix, [ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" } ])
  })
  afterEach(async () => {
    await deleteAll(s3, prefix)
  })
  it ('should delete one doc even if multiple docs match', async () => {
    let filter = { last: "Doe" }
    let result = await deleteOne(s3, prefix, filter)
    expect(result).toEqual({
      "deletedCount": 1,
      "explain": {
        "executionStats": {
          "nReturned": 2,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 2
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 2,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything(),
          "Deleted": [ { "Key": "test/delete/deleteOne/1" } ],
          "Errors": []
        }
      }
    })
  })
  it ('should not throw if none match', async () => {
    let filter = { last: "Smith" }
    let result = await deleteOne(s3, prefix, filter)
    expect(result).toMatchObject({ "deletedCount": 0 })
  })
})

describe('deleteAll', () => {
  let instance = 'test'
  let db = 'delete'
  let collection = 'deleteAll'
  let prefix = `${instance}/${db}/${collection}/`
  beforeEach(async () => {
    await insertMany(s3, prefix, [ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" } ])
  })
  afterEach(async () => {
    await deleteAll(s3, prefix)
  })
  it ('should delete all in a collection', async () => {
    let result = await deleteAll(s3, prefix)
    expect(result).toEqual({
      "deletedCount": 2,
      "explain": {
        "executionStats": {
          "nReturned": 2,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 2
        },
        "s3": {
          "IsTruncated": false,
          "KeyCount": 2,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything(),
          "Deleted": [
            { "Key": "test/delete/deleteAll/1" },
            { "Key": "test/delete/deleteAll/2" }
          ],
          "Errors": []
        }
      }
    })
  })
})

describe('deleteById', () => {
  let instance = "test"
  let db = 'delete'
  let collection = 'deleteById'
  let prefix = `${instance}/${db}/${collection}/`
  beforeEach(async () => {
    await insertMany(s3, prefix, [ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" } ])
  })
  afterEach(async () => {
    await deleteAll(s3, prefix)
  })
  it ('should delete a single doc', async () => {
    let result = await deleteById(s3, prefix, "1")
    expect(result).toEqual( {
      "deletedCount": 1,
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
          "MaxKeys": null,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything(),
          "Deleted": [
            {
              "Key": "test/delete/deleteById/1"
            }
          ],
          "Errors": []
        }
      }
    })
  })
  it ('should not throw if id does not exist', async () => {
    let result = await deleteById(s3, prefix, "BAD")
    // TODO: This is probably a bug in the underlying s3 package 
    // that deleting a nonexistent keys looks identical to deleting a real key. 
    // Deleted count should be 0
    expect(result).toMatchObject({
      "deletedCount": 1
    })
  })
})