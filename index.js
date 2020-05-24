const AWS = require('aws-sdk')
const createS3 = require('@songodb/songodb-s3')
const { Instance } = require('./lib/instance')

module.exports = exports = create

function create(awss3, bucket, { instanceId, dbName, collectionName }) {
  let instance = new Instance(createS3(awss3, bucket), instanceId)
  let db = null
  let collection = null
  if (dbName) {
    db = instance.db(dbName)
  }
  if (db && collectionName) {
    collection = db.collection(collectionName)
  }
  return { instance, db, collection }
}


(new AWS.S3(), process.env.BUCKET)