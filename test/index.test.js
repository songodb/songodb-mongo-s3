require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('../index')

describe('createSongoDB', () => {
  let instanceId = "index"
  let instance = null
  beforeEach(async () => {
    let songodb = createSongoDB(new AWS.S3(), process.env.BUCKET, { instanceId })
    instance = songodb.instance
  })
  afterEach(async () => {
    await instance.db('db1').dropDatabase()
  })
  it ('should create a valid instance', async () => {
    await instance.db('db1').collection('col1').insertOne({ hello: "world" })
  })
})