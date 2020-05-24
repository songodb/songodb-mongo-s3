const AWS = require('aws-sdk')
const createS3 = require('@songodb/songodb-s3')
const { Instance } = require('./lib/instance')

module.exports = exports = create

function create(bucket, awss3, { instanceId, dbName, collectionName }) {
  let s3 = createS3(awss3, bucket)
  let instance = new Instance(s3, instanceId)
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