const sift = require('sift')
const { createExplain } = require('./explain')

async function deleteMany(s3, prefix, filter, options) {
  options = options || { }
  if (filter["_id"] && (typeof filter["_id"] === 'string') && Object.keys(filter).length == 1) {
    // Shortcut if filter is just a plain id which means only one possible match to delete
    return await deleteById(s3, prefix, filter["_id"], options)
  }
  let t0 = Date.now()
  let scan = await s3.getPrefix(prefix, options)
  let docs = scan.Contents.map(content => content.Body)
  if (filter) {
    docs = docs.filter(sift(filter))
  }
  let explain = createExplain(scan, docs)
  explain.s3.Deleted = [ ]
  explain.s3.Errors = [ ]
  if (docs.length > 0) {
    if (options.limit) {
      docs = docs.slice(0, options.limit)
    }
    let keys = docs.map(doc => `${prefix}${doc["_id"]}`)
    let t2 = Date.now()
    let result = await s3.deleteMultiple(keys)
    let t3 = Date.now()
    explain.s3.Deleted = result.Deleted
    explain.s3.Errors = result.Errors
    explain.s3.TimeMillis += (t3 - t2)
  }
  let t1 = Date.now()
  explain.executionStats.executionTimeMillis = t1 - t0
  return {
    deletedCount: explain.s3.Deleted.length,
    explain
  }
}

async function deleteOne(s3, prefix, filter, options) {
  options = options || { }
  options.limit = 1
  return await deleteMany(s3, prefix, filter, options)
}

async function deleteById(s3, prefix, id, options) {
  options = options || { }
  let t0 = Date.now()
  let { Deleted, Errors } = await s3.deleteOne(`${prefix}${id}`)
  Deleted = Deleted && [ Deleted ] || [ ]
  let t1 = Date.now()
  let scan = {
    IsTruncated: false,
    KeyCount: Deleted.length,
    MaxKeys: options.MaxKeys || null,
    NextContinuationToken: null,
  }
  let docs = Deleted
  let explain = createExplain(scan, docs, { elapsed: t1 - t0, elapsedS3: t1 - t0 })
  explain.s3.Deleted = Deleted
  explain.s3.Errors = Errors
  return {
    "deletedCount": Deleted && 1 || 0,
    explain
  }
}

async function deleteAll(s3, prefix, options) {
  options = options || { }
  let t0 = Date.now()
  let scan = await s3.deletePrefix(prefix, options)
  let t1 = Date.now()
  let docs = scan.Contents.map(content => { return { } }) // content.Body is null for a plain list object
  let t2 = Date.now()
  let explain = createExplain(scan, docs, { elapsed: t2 - t0, elapsedS3: t1 - t0 })
  explain.s3.Deleted = scan.Deleted
  explain.s3.Errors = scan.Errors
  return {
    deletedCount: scan.Deleted.length,
    explain
  }
}

module.exports = exports = {
  deleteMany,
  deleteOne,
  deleteAll,
  deleteById
}