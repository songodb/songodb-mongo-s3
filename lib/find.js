const sift = require('sift')
const { sort, projection } = require('@songodb/mongojs')
const { createExplain } = require('./explain')

async function findMany(s3, prefix, query, options) {
  options = options || { }
  let t0 = Date.now()
  let { MaxKeys, ContinuationToken } = options 
  if (query && query["_id"] && (typeof query["_id"] === 'string') && Object.keys(query).length == 1) {
    // Shortcut: we avoid a "full table scan" when our query
    // is looking for a single record by only it's _id   
    scan = await findById(s3, prefix, query["_id"], { MaxKeys, ContinuationToken })
  } else {
    // we do a "full table scan" up to MaxKeys from the ContinuationToken onward
    scan = await s3.getPrefix(prefix, { MaxKeys, ContinuationToken })
  }
  const t1 = Date.now()
  let docs = scan.Contents.map(content => content.Body)
  if (query) {
    docs = docs.filter(sift(query))
  }
  if (options.sort) {
    docs = docs.sort(sort(options.sort))
  }
  if (options.skip) {
    docs = docs.slice(options.skip)
  }
  if (options.limit) {
    docs = docs.slice(0, options.limit)
  }
  if (options.projection) {
    docs = docs.map(doc => {
      let result = projection(options.projection)(doc)
      result["_id"] = doc["_id"]
      return result
    })
  }
  const t2 = Date.now()
  let explain = createExplain(scan, docs, { elapsed: t2 - t0, elapsedS3: t1 - t0 })
  return { docs, explain }
}

async function findOne(s3, prefix, query, options) {
  options = options || { }
  options.limit = 1
  return await findMany(s3, prefix, query, options)
}

async function findById(s3, prefix, id, options) {
  let doc = await s3.getOne(`${prefix}${id}`)
  return {
    IsTruncated: false,
    KeyCount: doc && 1 || 0,
    MaxKeys: options.MaxKeys,
    NextContinuationToken: null,
    Contents: doc && [ doc ] || [ ]
  }
}

module.exports = exports = {
  findMany,
  findOne,
  findById
}