const { isValid } = require('./validate')
const { Collection } = require('./collection')
const { listCollections, dropDatabase } = require('./system')

class Database {

  constructor(s3, instance, name, options) {
    this.s3 = s3
    this.instance = instance
    this.name = name
    this.prefix = `${instance.name}/${name}/`
    this.options = options || { }
  }

  checkValidNamespace() {
    return isValid({ db: this.name })
  }

  collection(name) {
    return new Collection(this.s3, this.instance, this, name, this.options)
  }

  async dropDatabase(options) {
    options = options || { }
    this.checkValidNamespace()
    return await dropDatabase(this.s3, this.prefix, options)
  }

  async listCollections(filter, options) {
    filter = filter || { }
    options = options || { }
    this.checkValidNamespace()
    return await listCollections(this.s3, this.prefix, filter, options)
  }
}

module.exports = exports = {
  Database
}