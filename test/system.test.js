require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {  
  listCollections,
  listDatabases,
  dropDatabase,
  dropCollection
  // checkDbExists,
  // createDb,
  // deleteDb,
  // checkCollectionExists,
  // createCollection,
  // deleteCollection
} = require('../lib/system')
const { Instance } = require('../lib/instance')
const { Database } = require('../lib/database')

describe('listCollections', () => {
  let instance = "system"
  let name = "listCollections"
  let db = null
  beforeEach(async () => {
    db = new Database(s3, { name: instance }, name)
    await db.collection('col1').insertOne({ hello: "world" })
    await db.collection('col2').insertOne({ foo: "bar" })
  })
  afterEach(async () => {
    await db.dropDatabase()
  })
  it ('should list all collections', async () => {
    let result = await db.listCollections()
    expect(result).toEqual({
      "docs": [ { "name": "col1" }, { "name": "col2" } ],
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
          "TimeMillis": expect.anything()
        }
      }
    })
  })
  it ('should return nameOnly', async () => {
    let result = await db.listCollections({ }, { nameOnly: true })
    expect(result).toMatchObject({ "docs": [ "col1", "col2" ] })
  })
  it ('should let filter by name', async () => {
    let result = await db.listCollections({ name: { "$in": [ "col1" ] } })
    expect(result).toMatchObject({ "docs": [ { "name": "col1" } ] })
  })
})

describe("listDatabases", () => {
  let name = "system"
  let instance = null
  beforeEach(async () => {
    instance = new Instance(s3, name)
    await instance.db('db1').collection('col1').insertOne({ hello: "world" })
    await instance.db('db2').collection('col2').insertOne({ foo: "bar" })
  })
  afterEach(async () => {
    await instance.db('db1').dropDatabase()
    await instance.db('db2').dropDatabase()
  })
  it ('should list all databases', async () => {
    let result = await instance.listDatabases()
    expect(result).toEqual({
      "docs": [ { "name": "db1" }, { "name": "db2" } ],
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
          "TimeMillis": expect.anything()
        }
      }
    })
  })
  it ('should return nameOnly', async () => {
    let result = await instance.listDatabases({ }, { nameOnly: true })
    expect(result).toMatchObject({ "docs": [ "db1", "db2" ] })
  })
  it('should let filter by name', async () => {
    let result = await instance.listDatabases({ name: { "$in": [ "db1" ] } })
    expect(result).toMatchObject({ "docs": [ { "name": "db1" } ] })
  })
})

// describe('checkDbExists', () => {
//   let instance = "systeminstance"
//   let db = "checkDbExists"
//   beforeEach(async () => {

//   })
//   afterEach(async () => {
//     await deleteDb(s3, { instance, db })
//   })
//   it ('should return false if db does not exist', async () => {
//     expect(await checkDbExists(s3, { instance, db })).toBe(false)
//   })
//   it ('should return name of db if it does exist', async () => {
//     await createDb(s3, { instance, db })
//     expect(await checkDbExists(s3, { instance, db })).toBe(db)
//   })
// })

// describe('createDb', () => {
//   let instance = "systeminstance"
//   let db = "createDb"
//   beforeEach(async () => {

//   })
//   afterEach(async () => {
//     await deleteDb(s3, { instance, db })
//   })
//   it ('should create a fresh db', async () => {
//     let result = await createDb(s3, { instance, db })
//     expect(await checkDbExists(s3, { instance, db })).toBe(db)
//     // console.log(JSON.stringify(result))
//     // {"insertedCount":1,"ops":[{"_id":"createDb","options":{}}],"insertedId":"createDb"}
//   })
//   it ('should not throw if db already exists', async () => {
//     await createDb(s3, { instance, db })
//     let result = await createDb(s3, { instance, db })
//     expect(await checkDbExists(s3, { instance, db })).toBe(db)
//     // console.log(JSON.stringify(result))
//     // {"insertedCount":1,"ops":[{"_id":"createDb","options":{}}],"insertedId":"createDb"}
//   })
// })

// describe('deleteDb', () => {
//   let instance = "systeminstance"
//   let db = "createDb"
//   beforeEach(async () => {
//     await createDb(s3, { instance, db })
//   })
//   afterEach(async () => {
//     await deleteDb(s3, { instance, db })
//   })
//   it ('should delete an existing db', async () => {
//     let result = await deleteDb(s3, { instance, db })
//     expect(await checkDbExists(s3, { instance, db })).toBe(false)
//     // console.log(JSON.stringify(result))
//     // {"Deleted":{"Key":"systeminstance/admin/databases/createDb"},"Errors":[]}
//   })
//   it ('should not throw if delete non-existent db', async () => {
//     await deleteDb(s3, { instance, db })
//     let result = await deleteDb(s3, { instance, db })
//     // console.log(JSON.stringify(result))
//     // {"Deleted":{"Key":"systeminstance/admin/databases/createDb"},"Errors":[]}
//   })
// })

// describe('checkCollectionExists', () => {
//   let instance = "systeminstance"
//   let db = "system"
//   let collection = "checkCollectionExists"
//   beforeEach(async () => {

//   })
//   afterEach(async () => {
//     await deleteCollection(s3, { instance, db, collection })
//   })
//   it ('should return false if collection does not exist', async () => {
//     expect(await checkCollectionExists(s3, { instance, db, collection })).toBe(false)
//   })
//   it ('should return name of collection if it does exist', async () => {
//     await createCollection(s3, { instance, db, collection })
//     expect(await checkCollectionExists(s3, { instance, db, collection })).toBe(collection)
//   })
// })

// describe('createCollection', () => {
//   let instance = "systeminstance"
//   let db = "system"
//   let collection = "createCollection"
//   beforeEach(async () => {

//   })
//   afterEach(async () => {
//     await deleteCollection(s3, { instance, db, collection })
//   })
//   it ('should create a fresh collection', async () => {
//     let result = await createCollection(s3, { instance, db, collection })
//     expect(await checkCollectionExists(s3, { instance, db, collection })).toBe(collection)
//     // console.log(JSON.stringify(result))
//     // {"insertedCount":1,"ops":[{"_id":"createCollection","options":{}}],"insertedId":"createCollection"}
//   })
//   it ('should not throw if collection already exists', async () => {
//     await createCollection(s3, { instance, db, collection })
//     let result = await createCollection(s3, { instance, db, collection })
//     expect(await checkCollectionExists(s3, { instance, db, collection })).toBe(collection)
//     // console.log(JSON.stringify(result))
//     // {"insertedCount":1,"ops":[{"_id":"createCollection","options":{}}],"insertedId":"createCollection"}
//   })
// })

// describe('deleteCollection', () => {
//   let instance = "systeminstance"
//   let db = "system"
//   let collection = "deleteCollection"
//   beforeEach(async () => {
//     await createCollection(s3, { instance, db, collection })
//   })
//   afterEach(async () => {
//     await deleteCollection(s3, { instance, db, collection })
//   })
//   it ('should delete an existing collection', async () => {
//     let result = await deleteCollection(s3, { instance, db, collection })
//     expect(await checkCollectionExists(s3, { instance, db, collection })).toBe(false)
//     // console.log(JSON.stringify(result))
//     // {"Deleted":{"Key":"systeminstance/system/system/namespaces/deleteCollection"},"Errors":[]}
//   })
//   it ('should not throw if delete non-existent collection', async () => {
//     await deleteCollection(s3, { instance, db, collection })
//     let result = await deleteCollection(s3, { instance, db, collection })
//     // console.log(JSON.stringify(result))
//     // {"Deleted":{"Key":"systeminstance/system/system/namespaces/deleteCollection"},"Errors":[]}
//   })
// })