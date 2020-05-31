require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const {
  Collection
} = require('../lib/collection')

describe("namespace", () => {
  it ('should throw on invalid namespace', async () => {
    let instance = "test"
    let db = "okay"
    let name = "$bad"
    let error = null
    let collection = new Collection(s3, { name: instance }, { name: db }, name)
    try {
      await collection.insertOne({ hello: "world" })
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.code).toEqual("InvalidNamespace")
    expect(error.message).toEqual(`Invalid collection name specified '${name}'`)
  })
})

describe("delete", () => {
  let instance = "test"
  let db = "collection"
  let name = "insert"
  let collection = null
  beforeEach(async () => {
    collection = new Collection(s3, { name: instance }, { name: db }, name)
    await collection.insertMany([ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } 
    ])
  })
  afterEach(async () => {
    await collection.drop()
  })
  it ('should deleteMany', async () => {
    let result = await collection.deleteMany({ first: { "$in": [ "Jane", "John" ] } })
    expect(result).toMatchObject({ deletedCount: 2})
  })
  it ('should deleteOne', async () => {
    let result = await collection.deleteOne({ first: { "$in": [ "Jane", "John" ] } })
    expect(result).toMatchObject({ deletedCount: 1 })
  })
  it ('should drop entire collection', async () => {
    let result = await collection.drop()
    expect(result).toMatchObject({ deletedCount: 3 })
  })
})

describe("find", () => {
  let instance = "test"
  let db = "collection"
  let name = "find"
  let collection = null
  beforeEach(async () => {
    collection = new Collection(s3, { name: instance }, { name: db }, name)
    await collection.insertMany([ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } 
    ])
  })
  afterEach(async () => {
    await collection.drop()
  })
  it ('should find', async () => {
    let result = await collection.find({ first: { "$in": [ "Jane", "John" ] } })
    expect(result.docs.length).toBe(2)
  })
  it ('should findOne and return obj in doc field', async () => {
    let result = await collection.findOne({ first: "Jane" })
    expect(result.doc).toMatchObject({ first: "Jane" })
  })
  it ('should throw on invalid query', async () => {
    let error = null
    try {
      await collection.find({ first: { "$bad": [ "Jane", "John" ] } })
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.message).toEqual("Unsupported operation: $bad")
  })
})

describe("insert", () => {
  let instance = "test"
  let db = "collection"
  let name = "insert"
  let collection = null
  beforeEach(async () => {
    collection = new Collection(s3, { name: instance }, { name: db }, name)
  })
  afterEach(async () => {
    await collection.drop()
  })
  it ('should insertMany', async () => {
    let result = await collection.insertMany([ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } 
    ])
    expect(result).toMatchObject({ insertedCount: 3})
  })
  it ('should insertOne', async () => {
    let result = await collection.insertOne({ _id: "1", first: "Jane", last: "Doe" })
    expect(result).toMatchObject({ insertedCount: 1 })
  })
})

describe("update", () => {
  let instance = "test"
  let db = "collection"
  let name = "update"
  let collection = null
  beforeEach(async () => {
    collection = new Collection(s3, { name: instance }, { name: db }, name)
    await collection.insertMany([ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } 
    ])
  })
  afterEach(async () => {
    await collection.drop()
  })
  it ('should updateMany', async () => {
    let result = await collection.updateMany(
      { first: { "$in": [ "Jane", "John" ] } },
      { "$set": { last: "Smith" } }
    )
    expect(result.modifiedCount).toBe(2)
  })
  it ('should updateOne', async () => {
    let result = await collection.updateOne(
      { first: { "$in": [ "Jane", "John" ] } },
      { "$set": { last: "Smith" } }
    )
    expect(result.modifiedCount).toBe(1)
  })
  it ('should throw on bad formatted update doc', async () => {
    let error = null
    try {
      await collection.updateMany(
        { first: { "$in": [ "Jane", "John" ] } },
        { last: "BadUpdate" } )
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.message).toBe("Unsupported operation: last")
  })
  it ('should replaceOne', async () => {
    let result = await collection.replaceOne({ first: "Jane" }, { hello: "world" })
    expect(result.modifiedCount).toBe(1)
  })
})