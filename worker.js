var http = require('http');
var amq = require('amq');
var config = require('./workerConfig');
var request = require('request');
var Re = require('re');

http.globalAgent.maxSockets = 5;

module.exports = Worker;

function Worker(queueName, delHost, delPort, resTimeout, maxRetries) {

  var self = this;

  self._hostName = delHost;
  self._port = delPort;

  self._deleter = self._Deleter();

  self._deleter.on('error', function(err){
    console.log(err)
  });
  self._toBeDeleted = {};
  self._unAckedMessages = {};
  self._deleter.listen(self._port, self._hostName);

  self._maxRetries = maxRetries || 30;

  self._timeout = resTimeout || 60000;

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
    self._unAckedMessages[uniqueId] = message;

    // nack unparsable messages
    if (!message || !message.content || !message.content.toString()) {
      if (self._unAckedMessages[uniqueId]) {
        delete self._unAckedMessages[uniqueId];
        queue.nack(message, false, false);
      }
      return;
    }

    var jsonContent = JSON.parse(message.content.toString());

    //nack messages that are missing and endpoint or a payload
    if (!jsonContent.endpoint || !jsonContent.payload) {
      if (self._unAckedMessages[uniqueId]) {
        delete self._unAckedMessages[uniqueId];
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
        'Iron-Subscriber-Message-Url': "http://" + self._hostName + ":" + self._port + "/" + uniqueId
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

        if (error) {
          console.log('trying for the ' + count + " time");
          done(error);
          //if (count >= 19) {
          //  incrementRetry();
          //  console.log("requeuing message to" + jsonContent.endpoint + " with retryCount: " + jsonContent.retryCount);
          //}
        }

        else if (response.statusCode == 200) {
          //setImmediate(function(mess){queue.ack(mess)}, message);
          if (self._unAckedMessages[uniqueId]) {
            delete self._unAckedMessages[uniqueId];
            queue.ack(message);
          }
          // console.log("got a 200 " + new Date());
          done(null, count);
        }

        else if (response.statusCode == 202) {
          var timeout = (body.timeout * 1000) || self._timeout;
          var tp = setTimeout(incrementRetry, timeout);
          self._toBeDeleted[uniqueId] = tp;
          self._unAckedMessages[uniqueId] = message;
          done(null, count);
        }
        else {
          incrementRetry();
          console.log(response.statusCode + " from " + jsonContent.endpoint + " with retryCount: " + jsonContent.retryCount);
          done(null, count);
        }
      });
    }

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
      jsonContent.retryCount = jsonContent.retryCount ? jsonContent.retryCount + 1 : 1;
      if (jsonContent.retryCount < self._maxRetries) {
        queue.publish(JSON.stringify(jsonContent), {deliveryMode: true, mandatory: true});
      }

      delete self._toBeDeleted[uniqueId];
      if (self._unAckedMessages[uniqueId]) {
        var mess = self._unAckedMessages[uniqueId];
        delete self._unAckedMessages[uniqueId];
        if (!mess) {
          console.log(mess);
        }
        queue.ack(mess);
      }
    }

  });
};

Worker.prototype._Deleter = function(){
  var self = this;

  return http.createServer(function (req, res) {
    if (req.method == 'DELETE') {
      parsedUrl = require('url').parse(req.url);
      var deleteId = parsedUrl.path.substr(1);
      if (self._toBeDeleted[deleteId]) {
        clearTimeout(self._toBeDeleted[deleteId]);
        self._queue.ack(self._unAckedMessages[deleteId]);
          delete self._unAckedMessages[deleteId];

        delete self._toBeDeleted[deleteId];

        res.writeHead(200, {'Content-Type': "application/json; charset=UTF-8"});
        res.write('{"msg": "Deleted"}');
        res.end();
      }
      else {
        res.writeHead(403, {'Content-Type': "application/json; charset=UTF-8"});
        res.write('{"msg" : "Not Reserved"');
        res.end();
      }
    }
  });
};