const { isValid } = require('./validate')
const { deleteMany, deleteOne } = require('./delete')
const { dropCollection } = require('./system')
const { insertMany, insertOne } = require('./insert')
const { findMany, findOne } = require('./find')
const { updateMany, updateOne, replaceOne } = require('./update')

class Collection {

  constructor(s3, instance, db, name, options) {
    this.s3 = s3
    this.instance = instance
    this.db = db
    this.name = name
    this.prefix = `${instance.name}/${db.name}/${name}/`
    this.options = options || { }
  }

  checkValidNamespace() {
    return isValid({ db: this.db.name, collection: this.name })
  }

  async deleteMany(filter, options) {
    options = options || { }
    this.checkValidNamespace()
    return await deleteMany(this.s3, this.prefix, filter, options)
  }

  async deleteOne(filter, options) {
    options = options || { }
    this.checkValidNamespace()
    return await deleteOne(this.s3, this.prefix, filter, options)
  }

  async drop(options) {
    options = options || { }   
    this.checkValidNamespace()
    return await dropCollection(this.s3, this.prefix, options)
  }

  async find(query, options) {
    options = options || { }
    this.checkValidNamespace()
    return await findMany(this.s3, this.prefix, query, options)
  }

  async findOne(query, options) {
    options = options || { }
    this.checkValidNamespace()
    let result = await findOne(this.s3, this.prefix, query, options)
    result.doc = result.docs[0] || null
    delete result.docs
    return result
  }

  async insertMany(docs, options) {
    options = options || { }
    this.checkValidNamespace()
    return await insertMany(this.s3, this.prefix, docs, options)
  }

  async insertOne(doc, options) {
    options = options || { }
    this.checkValidNamespace()
    return await insertOne(this.s3, this.prefix, doc, options)
  }

  async replaceOne(filter, replace, options) {
    options = options || { }
    this.checkValidNamespace()
    return await replaceOne(this.s3, this.prefix, filter, replace, options)
  }
  async updateMany(filter, update, options) {
    options = options || { }
    this.checkValidNamespace()
    return await updateMany(this.s3, this.prefix, filter, update, options)
  }

  async updateOne(filter, update, options) {
    options = options || { }
    this.checkValidNamespace()
    return await updateOne(this.s3, this.prefix, filter, update, options)
  }
}

module.exports = exports = {
  Collection
}