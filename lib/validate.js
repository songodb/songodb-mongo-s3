function isValid({ db, collection }, options) {
  options = options || { }
  if (db && !isValidDbName({ db }, options)) {
    let error = new Error(`Invalid database name specified '${db}'`)
    error.code = "InvalidNamespace"
    throw error
  }
  if (collection && !isValidCollectionName({ db, collection }, options)) {
    let error = new Error(`Invalid collection name specified '${collection}'`)
    error.code = "InvalidNamespace"
    throw error
  }
  return true
}

function checkLength({ db, collection }) {
  if (db && db.length >= 64) return false
  if (db && collection && `${db}.${collection}`.length > 120) return false 
}

const ILLEGAL_CHARACTERS = /[\\\.\s\"\$\*\<\>\:\|\?]/
const COLLECTION_START = /^[_a-zA-Z]/

function isValidDbName({ db }, options) {
  if (!db) return false
  if (db.length >= 64) return false
  if (db.match(ILLEGAL_CHARACTERS)) return false
  return true
}

function isValidCollectionName({ db, collection }, options) {
  if (!collection) return false
  if (`${db}.${collection}`.length > 120) return false
  if (!collection.match(COLLECTION_START)) return false 
  if (collection.indexOf("$") >= 0) return false
  if (collection.startsWith("system")) return false
  if (collection.match(ILLEGAL_CHARACTERS)) return false
  return true
}

function isValidJSON(str) {
  try {
    JSON.parse(str)
    return true
  } catch (err) {
    return false
  }
}

module.exports = exports = {
  isValid,
  isValidDbName,
  isValidCollectionName,
  isValidJSON
}

/**
 * 
 * db.getCollection('$.asupereillga').find({})
 * 
 * Error: error: {
	"operationTime" : Timestamp(1590338787, 1),
	"ok" : 0,
	"errmsg" : "Invalid collection name specified 'test.$.asupereillga'",
	"code" : 73,
	"codeName" : "InvalidNamespace",
	"$clusterTime" : {
		"clusterTime" : Timestamp(1590338787, 1),
		"signature" : {
			"hash" : BinData(0,"d2yq64lQb0y5BCyhcmCfiZprUdU="),
			"keyId" : NumberLong("6800076575631998977")
		}
	}
}

 */