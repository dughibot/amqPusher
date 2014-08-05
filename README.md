amqPusher
=========

worker to turn make an ironIO subscriber work with rabbitmq

Setting up worker requires 2 arguments: the hostName and port for the reserved message deletion server.
The 3 optional arguments are: the queue name, then the default timeout for reserved messages, then the maximum number
  of times a message will be requeued because of an error.