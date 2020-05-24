const sift = require('sift')
const { deleteAll } = require('./delete')
const { createExplain } = require('./explain')

async function dropCollection(s3, prefix, options) {
  options = options || { }
  let result = await deleteAll(s3, prefix, options)
  result.dropped = !result.explain.s3.IsTruncated
  return result
}

async function dropDatabase(s3, prefix, options) {
  options = options || { }
  let result = await deleteAll(s3, prefix, options)
  result.dropped = !result.explain.s3.IsTruncated
  return result
}

async function listCollections(s3, prefix, filter, options) {
  options = options || { }
  let t0 = Date.now()
  options.Delimiter = '/'
  let scan = await s3.list(prefix, options)
  let t1 = Date.now()
  let docs = scan.CommonPrefixes.map(({ Prefix }) => { 
    return {
      name: extractName(prefix, Prefix, options.Delimiter) 
    }
  })
  if (filter) {
    docs = docs.filter(sift(filter))
  }
  if (options.nameOnly) {
    docs = docs.map(doc => doc.name)
  }
  let t2 = Date.now()
  let explain = createExplain(scan, docs, { elapsed: t2 - t0, elapsedS3: t1 - t0 })
  return { docs, explain }
}

async function listDatabases(s3, prefix, filter, options) {
  options = options || { }
  // same implementation just a different prefix
  return await listCollections(s3, prefix, filter, options)
}

async function destroyInstance(s3, prefix, options) {
  // Not implemented
  return null
}

function extractName(prefix, common, delimiter) {
  return common.substring(prefix.length, common.length - delimiter.length)
}


// async function checkDbExists(s3, { instance, db }) {
//   let key = `${instance}/admin/databases/${db}`
//   try {
//     let data = await s3.getOne(key)
//     return data && data.Body && data.Body["_id"] || false
//   } catch (err) {
//     if (err.code == "NoSuchKey") return false
//     throw err
//   }
// }

// async function checkCollectionExists(s3, { instance, db, collection }) {
//   let key = `${instance}/${db}/system/namespaces/${collection}`
//   try {
//     let data = await s3.getOne(key)
//     return data && data.Body && data.Body["_id"] || false
//   } catch (err) {
//     if (err.code == "NoSuchKey") return false
//     throw err
//   }
// }

// async function createDb(s3, { instance, db }, options) {
//   options = options || { }
//   let prefix = `${instance}/admin/databases/`
//   let doc = { "_id": db, options }
//   return await insertOne(s3, prefix, doc)
// }

// async function createCollection(s3, { instance, db, collection }, options) {
//   options = options || { }
//   let prefix = `${instance}/${db}/system/namespaces/`
//   let doc = { "_id": collection, options }
//   return await insertOne(s3, prefix, doc)
// }

// async function deleteDb(s3, { instance, db }, options) {
//   options = options || { }
//   let key = `${instance}/admin/databases/${db}`
//   try {
//     return await s3.deleteOne(key)
//   } catch (err) {
//     if (err.code != "NoSuchKey") throw err
//   }
// }

// async function deleteCollection(s3, { instance, db, collection }, options) {
//   options = options || { }
//   let key = `${instance}/${db}/system/namespaces/${collection}`
//   try {
//     return await s3.deleteOne(key)
//   } catch (err) {
//     if (err.code != "NoSuchKey") throw err
//   }
// }

module.exports = exports = {
  listDatabases,
  dropDatabase,
  listCollections,
  dropCollection,
  destroyInstance
  // checkDbExists,
  // checkCollectionExists,
  // createDb,
  // createCollection,
  // deleteDb,
  // deleteCollection
}