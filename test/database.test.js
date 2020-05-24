require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {
  Database
} = require('../lib/database')

describe("namespace", () => {
  it ('should throw on invalid namespace', async () => {
    let instance = "test"
    let name = "$bad"
    let error = null
    let db = new Database(s3, { name: instance }, name)
    try {
      await db.listCollections()
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.code).toEqual("InvalidNamespace")
    expect(error.message).toEqual(`Invalid database name specified '${name}'`)
  })
})

describe("listCollections", () => {
  let instance = "test"
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
    expect(result).toMatchObject({
      "docs": [ { "name": "col1" }, { "name": "col2" } ],
    })
  })
})

describe("dropDatabase", () => {
  let instance = "test"
  let name = "dropDatabase"
  let db = null
  beforeEach(async () => {
    db = new Database(s3, { name: instance }, name)
    await db.collection('col1').insertOne({ hello: "world" })
    await db.collection('col2').insertOne({ foo: "bar" })
  })
  afterEach(async () => {
    await db.dropDatabase()
  })
  it ('should dropDatabase', async () => {
    let result = await db.dropDatabase()
    expect(result).toMatchObject({
      deletedCount: 2,
      dropped: true
    })
  })
})
