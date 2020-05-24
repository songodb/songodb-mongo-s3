require('dotenv').config()
const { isValid, isValidDbName, isValidCollectionName, isValidJSON } = require('../lib/validate')

describe('isValid', () => {
  it ('should throw if not valid db name', async () => {
    let db = "hello world"
    let error = null
    try {
      isValid({ db })
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.code).toEqual("InvalidNamespace")
    expect(error.message).toEqual("Invalid database name specified 'hello world'")
  })
  it ('should throw if not valid collection name', async () => {
    let db = "okay"
    let collection = "$bad"
    let error = null
    try {
      isValid({ db, collection })
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.code).toEqual("InvalidNamespace")
    expect(error.message).toEqual("Invalid collection name specified '$bad'")
  })
  it ('should return true if db and collection name is valid', async () => {
    let db = "okay"
    let collection = "alsookay"
    expect(isValid({ db, collection })).toBe(true)
  })
})

describe('isValidDbName', () => {
  it ('should validate a simple name', async () => {
    let db = "okDbName"
    expect(isValidDbName({ db })).toBe(true)
  })
  it ('should not allow whitespace', async () => {
    let db = "hello world"
    expect(isValidDbName({ db })).toBe(false)
  })
  it ('should not allow $', async () => {
    let db = "hello$world"
    expect(isValidDbName({ db })).toBe(false)
  })
  it ('should not allow length of 64 or greater', async () => {
    let db = "helloworldhelloworldhelloworldhelloworldhelloworldhelloworld1234"
    expect(isValidDbName({ db })).toBe(false)
  })
})

describe('isValidCollectionName', () => {
  it ('should validate a simple name', async () => {
    let db = "mydb"
    let collection = "okCollection"
    expect(isValidCollectionName({ db, collection })).toBe(true)
  })
  it ('should not allow whitespace', async () => {
    let db = "mydb"
    let collection = "bad collection"
    expect(isValidCollectionName({ db, collection })).toBe(false)
  })
  it ("should not start with 'system'", async () => {
    let db = "mydb"
    let collection = "systemCollection"
    expect(isValidCollectionName({ db, collection })).toBe(false)
  })
  it ("should not start with a number", async () => {
    let db = "mydb"
    let collection = "1collection"
    expect(isValidCollectionName({ db, collection })).toBe(false)
  })
})

describe('isValidJSON', () => {
  it ('should be valid JSON', async () => {
    expect(isValidJSON(JSON.stringify({ hello: "world" }))).toBe(true)
  })
  it ('should not be valid JSON', async () => {
    expect(isValidJSON("{ hello: \"world\"")).toBe(false)
  })
  it ('null is valid JSON', async () => {
    expect(isValidJSON(null)).toBe(true)
  })
  it ('undefined is not valid JSON', async () => {
    expect(isValidJSON(undefined)).toBe(false)
  })
})




