const { Database } = require('./database')
const { listDatabases, destroyInstance } = require('./system')

class Instance {

  constructor(s3, name, options) {
    this.s3 = s3
    this.name = name
    this.prefix = `${name}/`
    this.options = options || { }
  }

  checkValidNamespace() {
    // No definition yet of valid instanceId
    return true
  }

  db(name) {
    return new Database(this.s3, this, name, this.options)
  }

  async destroy(options) {
    options = options || { }
    this.checkValidNamespace()
    return await destroyInstance(this.s3, this.prefix, options)
  }

  async listDatabases(filter, options) {
    filter = filter || { }
    options = options || { }
    this.checkValidNamespace()
    return await listDatabases(this.s3, this.prefix, filter, options)
  }
}

module.exports = exports = {
  Instance
}