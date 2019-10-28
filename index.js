/// GLOBAL RETHINKDB:
const r = require.main.rethinkdb || require('rethinkdb')
module.exports = r

/// AUTOMATIC RECONNECT:
class AutoConnection {
  constructor(settings) {
    this.settings = settings
    this.connected = false
    this.connect()
  }

  async connect() {
    if(this.promise) return this.promise
    this.promise = new Promise((resolve, reject) => {
      let tryCount = 0
      const tryConnect = () => {
        console.log("RETHINKDB CONNECTING.")
        r.connect(this.settings).then(connection => {
          console.log("RETHINKDB CONNECTED.")
          this.connected = true
          resolve(connection)
        }).catch(err => {
          console.error("RECONNECT FAILED", err && err.msg)
          let delay = Math.min( 100 * Math.pow(2, tryCount), 2000)
          console.error(`RETRYING IN ${delay} MILISECONDS.`)
          tryCount++
          setTimeout(() => tryConnect(), delay)
        })
      }
      tryConnect()
    })
    return this.promise
  }

  reconnect() {
    console.error(`RETHINKDB RECONNECTING!`)
    this.connected = false
    this.promise = null
    this.connect()
  }

  handleDisconnectError(error) {
    let disconnected = false
    if(error.msg == 'Connection is closed.' ) disconnected = true
    if(disconnected) {
      console.error(`RETHINKDB DISCONNECTED!`)
      if (this.connected) {
        this.reconnect()
      }
    }
    return disconnected
  }
}


let connections = new Map()

r.autoConnection = function(settings) {
  if(!settings) settings = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    db: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    timeout: process.env.DB_TIMEOUT
  }
  const key = JSON.stringify(settings)
  const existing = connections.get(key)
  if(existing) return existing
  const connection = new AutoConnection(settings)
  connections.set(key, connection)
  return connection
}