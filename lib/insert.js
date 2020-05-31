const ObjectID = require("bson-objectid")
const { createExplain } = require('./explain')

// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertOneWriteOpResult
async function insertOne(s3, prefix, doc, options) {
  options = options || { }
  let t0 = Date.now()
  doc["_id"] = doc["_id"] || ObjectID.generate()
  let key = `${prefix}${doc["_id"]}`
  let result = await s3.putOne(key, doc)
  let t1 = Date.now()
  let explain = createExplain(blankScan(), [ ], { elapsed: t1-t0, elapsedS3: t1-t0 })
  return {
    insertedCount: 1,
    ops: [ doc ],
    insertedId: doc["_id"],
    explain
  }
}

// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertWriteOpResult
async function insertMany(s3, prefix, docs, options) {  
  options = options || { }
  let t0 = Date.now()
  docs.forEach(doc => { doc["_id"] = doc["_id"] || ObjectID.generate() })
  let keys = docs.map(doc => `${prefix}${doc["_id"]}`)
  let result = await s3.putMultiple(keys, docs)
  let t1 = Date.now()
  let explain = createExplain(blankScan(), [ ], { elapsed: t1-t0, elapsedS3: t1-t0 })
  return {
    insertedCount: result.length,
    ops: docs,
    insertedIds: docs.map(doc => doc["_id"]),
    explain
  }
}


function blankScan() {
  return {
    IsTruncated: false,
    KeyCount: 0,
    MaxKeys: null,
    NextContinuationToken: null,
    TimeMillis: -1
  }
}


module.exports = exports = {
  insertOne, 
  insertMany
}