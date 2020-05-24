const deepequal = require('deep-equal')
const { findMany } = require('./find')
const { insertOne } = require('./insert')
const { update } = require('@songodb/mongojs')

async function updateMany(s3, prefix, filter, up, options) {
  options = options || { }
  let t0 = Date.now()
  let { docs, explain } = await findMany(s3, prefix, filter, options)
  if (docs.length == 0 && options.upsert) {
    let t3 = Date.now()
    let result = await upsert(s3, prefix, up, options)
    let t4 = Date.now()
    explain.executionStats.executionTimeMillis = t4 - t0
    explain.s3.TimeMillis = explain.s3.TimeMillis + (t4 - t3)
    result.explain = explain
    return result
  }
  if (docs.length > 0 && options.limit) {
    docs = docs.slice(0, options.limit)
  }
  let copy = JSON.parse(JSON.stringify(docs))
  let updated = [ ]
  copy.forEach(update(up))
  for (let i=0; i<copy.length; i++) {
    if (!deepequal(copy[i], docs[i])) {
      updated.push(copy[i])
    }
  }
  let t1 = Date.now()
  if (updated.length > 0) {
    // Need to write the updated docs by _id so they overwrite originals
    await Promise.all(updated.map(doc => insertOne(s3, prefix, doc, options)))
  }
  let t2 = Date.now()
  // Update the explain from findMany to include the extra work
  // we've done for the update
  explain.executionStats.executionTimeMillis = t2 - t1
  explain.s3.TimeMillis = explain.s3.TimeMillis + (t2 - t1)
  return {
    matchedCount: docs.length,
    modifiedCount: updated.length, 
    upsertedCount: 0,
    upsertedId: null,
    explain,
  }
}

async function updateOne(s3, prefix, filter, up, options) {
  options = options || { }
  options.limit = 1
  return await updateMany(s3, prefix, filter, up, options)
}

async function replaceOne(s3, prefix, filter, replace, options) {
  options = options || { }
  let t0 = Date.now()
  let { docs, explain } = await findMany(s3, prefix, filter, {
    limit: 1,
    MaxKeys: options.MaxKeys,
    ContinuationToken: options.ContinuationToken
  })
  let t1 = Date.now()
  if (docs.length == 0 && options.upsert) {
    let t3 = Date.now()
    let result = await upsert(s3, prefix, { "$set": replace }, options)
    let t4 = Date.now()
    explain.executionStats.executionTimeMillis = t4 - t0
    explain.s3.TimeMillis = explain.s3.TimeMillis + (t4 - t3)
    result.explain = explain
    return result
  }
  let insert = null
  if (docs.length > 0) {
    replace["_id"] = docs[0]["_id"]
    insert = await insertOne(s3, prefix, replace)
  }
  let t2 = Date.now()
  explain.executionStats.executionTimeMillis = t2 - t0
  explain.s3.TimeMillis = explain.s3.TimeMillis + (t2 - t1)
  return {
    matchedCount: docs.length,
    modifiedCount: insert && 1 || 0, 
    upsertedCount: 0,
    upsertedId: null,
    explain
  }
}

// we insert the doc using the given update document
async function upsert(s3, prefix, up, options) {
  options = options || { }
  let doc = { }
  update(up)(doc) // apply the update to a blank doc
  let { insertedId } = await insertOne(s3, prefix, doc, options)
  return {
    matchedCount: 0,
    modifiedCount: 0, 
    upsertedCount: 1,
    upsertedId: { _id: insertedId }
  }
}

module.exports = exports = {
  updateMany,
  updateOne,
  replaceOne,
  upsert
}


// updateMany(filter, update, options, callback)
// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#updateMany
// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~updateWriteOpResult
// async function updateMany(s3, prefix, docs, up, options) {
//   options = options || { }
//   let copy = JSON.parse(JSON.stringify(docs))
//   let updated = [ ]
//   copy.forEach(update(up))
//   for (let i=0; i<copy.length; i++) {
//     if (!deepequal(copy[i], docs[i])) {
//       updated.push(copy[i])
//     }
//   }
//   // Need to write each one of these docs by id
//   let result = await Promise.all(updated.map(doc => insertOne(s3, prefix, doc, options)))
//   return {
//     matchedCount: docs.length,
//     // technically these shouldn't always be equal, only if the update 
//     // changed the object should it be considered modified
//     // we could manually do a diff between original and modified if we 
//     // really want get this count
//     modifiedCount: updated.length, 
//     upsertedCount: 0,
//     upsertedId: null
//   }
// }
