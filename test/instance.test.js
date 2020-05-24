require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {
  Instance
} = require('../lib/instance')

describe("listDatabases", () => {
  let name = "listDatabases"
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
})
