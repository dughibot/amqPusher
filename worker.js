//TODO: log stuff instead of using console.log

var http = require('http');
var amq = require('amq');
var config = require('./workerConfig');
var request = require('request');
var Re = require('re');

http.globalAgent.maxSockets = 5;

module.exports = Worker;

function Worker(delHost, delPort, queueName, resTime, maxRetries) {

  var self = this;

  // Set up the reserved message deletion server
  self._delHost = delHost;
  self._delPort = delPort;
  self._deleter = self._Deleter();
  self._deleter.on('error', function(err){
    //log error
    console.log(err)
  });
  self._deleter.listen(self._delPort, self._delHost);

  self._reserved = {};
  self._unAcked = {};

  self._maxRetries = maxRetries || 30;

  self._timeout = resTime || 60000;

  // Set up the queue
  self._connection = amq.createConnection(config.connOpts.amq, config.connOpts.sock);
  queueName = queueName || '';
  queueOpts = config.queueOpts;
  queueOpts.durable = queueOpts.durable || true;
  self._queue = self._connection.queue(queueName, queueOpts);

  self._handlersRoute = config.handlerRt;
}

Worker.prototype.eat = function () {
  var self = this;
  queue = self._queue;

  queue.consume(function(message) {
    var uniqueId = message.fields.deliveryTag;
    self._unAcked[uniqueId] = message;

    // nack unparsable messages
    if (!message || !message.content || !message.content.toString()) {
      if (self._unAcked[uniqueId]) {
        delete self._unAcked[uniqueId];
        queue.nack(message, false, false);
      }
      return;
    }

    var jsonContent = JSON.parse(message.content.toString());

    //nack messages that are missing and endpoint or a payload
    if (!jsonContent.endpoint || !jsonContent.payload) {
      if (self._unAcked[uniqueId]) {
        delete self._unAcked[uniqueId];
        queue.nack(message, false, false);
      }
      return;
    }

    // info for request to be sent to handler
    var handOpts = {
      uri: 'http://localhost:1338' + jsonContent.endpoint,
      method: 'POST',
      agent: false,
      headers: {
        'Content-Type': "application/json; charset=UTF-8",
        'connection': "keep-alive",
        'user-agent': "IronMQ Pusherd",
        'Iron-Message-Id': uniqueId,
        'Iron-Subscriber-Message-Id': uniqueId,
        'Iron-Subscriber-Message-Url': "http://" + self._delHost + ":" + self._delPort + "/" + uniqueId
      },
      json: jsonContent.payload
    };

    // function to be called after request succeeds or fails
    function requestSent(err, count) {
      if (err) {
        if (count > 10) {
          console.log(err);
        }
      }
      else {
        if (count > 0) {
          console.log("got through after " + count);
        }
      }
    }

    // function for sending request.
    function sendRequest(count, done) {
      request(handOpts, function (error, response, body) {

        // on error, log error
        if (error) {
          console.log('trying for the ' + count + " time");
          done(error);
        }

        // On 200, just ack and remove from unAcked object
        else if (response.statusCode == 200) {
          //setImmediate(function(mess){queue.ack(mess)}, message);
          if (self._unAcked[uniqueId]) {
            delete self._unAcked[uniqueId];
            queue.ack(message);
          }
          // console.log("got a 200 " + new Date());
          done(null, count);
        }

        // On 202, set up to republish the message after a timeout.
        else if (response.statusCode == 202) {
          var timeout = (body.timeout * 1000) || self._timeout;
          var tp = setTimeout(incrementRetry, timeout);
          self._reserved[uniqueId] = tp;
          self._unAcked[uniqueId] = message;
          done(null, count);
        }

        // On other response codes, requeue message with new error count and log error
        else {
          incrementRetry();
          console.log(response.statusCode + " from " + jsonContent.endpoint + " with retryCount: " + jsonContent.retryCount);
          done(null, count);
        }
      });
    }

    // options for the re module
    var reOptions = {
      retries : 20,
      strategy : {
        "type": Re.STRATEGIES.EXPONENTIAL,
        "initial":3000,
        "base":2,
        "rand": true
      }
    };
    var re = new Re(reOptions);

    // use re to auto retry if request failed.
    re.try(sendRequest, requestSent());


    function incrementRetry() {
      // update the retryCount and republish the message if it hasn't been retried too many times
      jsonContent.retryCount = jsonContent.retryCount ? jsonContent.retryCount + 1 : 1;
      if (jsonContent.retryCount < self._maxRetries) {
        queue.publish(JSON.stringify(jsonContent), {deliveryMode: true, mandatory: true});
      }

      // remove the republish from the object of timeouts to republish reserved messages
      delete self._reserved[uniqueId];

      // if the message hasn't been acked, ack it and remove it from the object of unacked messages
      if (self._unAcked[uniqueId]) {
        var mess = self._unAcked[uniqueId];
        delete self._unAcked[uniqueId];
        if (!mess) {
          emit(new Error("Somehow tried to ack undefined")); //TODO: Is this how this should work?
        }
        queue.ack(mess);
      }
    }

  });
};


// the server for deleting reserved messages
Worker.prototype._Deleter = function(){
  var self = this;

  return http.createServer(function (req, res) {
    if (req.method == 'DELETE') {

      // get the message's delivery tag
      parsedUrl = require('url').parse(req.url);
      var deleteId = parsedUrl.path.substr(1);

      // clear the republishing of the message, ack it, and delete it from the objects
      if (self._reserved[deleteId]) {
        clearTimeout(self._reserved[deleteId]);
        self._queue.ack(self._unAcked[deleteId]);
        delete self._unAcked[deleteId];

        delete self._reserved[deleteId];

        res.writeHead(200, {'Content-Type': "application/json; charset=UTF-8"});
        res.write('{"msg": "Deleted"}');
        res.end();
      }

      // if the message wasn't a reserved message, or was already republished, return a 403
      else {
        res.writeHead(403, {'Content-Type': "application/json; charset=UTF-8"});
        res.write('{"msg" : "Not Reserved"');
        res.end();
      }
    }
  });
};