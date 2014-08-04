/**
 * Created by loaner on 7/28/14.
 */
module.exports = {
  //reservedTimeout : 20000,
  handlerRt: "http://localhost:1338",
  //deleterHost: 'localhost',
  //deleterPort: 1337,
  //queueName : "someName",
  queueOpts : {
    durable : true,
    prefetch: 10000
  },
  connOpts: {
    amq: {
      host: 'localhost',
      debug: true//,
      //heartbeat: 10
    },
    sock: {
      reconnect: {
        strategy: 'constant',
        initial: 1000
      }
    }
  }
};