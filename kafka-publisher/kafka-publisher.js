const { Kafka } = require('kafkajs');

module.exports = function(RED) {
    var node;

    function pushData(config) {
        RED.nodes.createNode(this, config);

        node = this;

        node.kafkahost = config.kafkahost;
        node.kafkaport = config.kafkaport;
        node.kafkatopic = config.kafkatopic;
        node.kafkaconnectiontimeout = config.kafkaconnectiontimeout;
        node.kafkarequesttimeout = config.kafkarequesttimeout;

        var kafkaHost = node.kafkahost,
        kafkaPort = node.kafkaport,
        kafkaTopic = node.kafkatopic,
        kafkaConnectionTimeout = node.kafkaconnectiontimeout,
        kafkaRequestTimeout = node.kafkarequesttimeout;

        logMessage("Initialising on " + kafkaHost + ":" + kafkaPort);
        var kafka = new Kafka({
          clientId: 'kafka-publisher',
          brokers: [kafkaHost + ':' + kafkaPort],
          connectionTimeout: kafkaConnectionTimeout,
          requestTimeout: kafkaRequestTimeout,
          logCreator: errorReporterCreator
        });
        var producer = kafka.producer();

        producer.connect()

        node.on('input', function(msg, send, done) {
            logMessage("Sending message to " + kafkaTopic);

            producer.send({
              topic: kafkaTopic,
              messages: [
                { value: JSON.stringify(msg.payload) },
              ]
            })

            // if (done) {
            //   done();
            // }
        });
    }

    const errorReporterCreator = logLevel =>  {
      return function(info) {

        switch(info.label) {
          case 'ERROR':
            logMessage(info.label + ": " + info.log.message);

            node.error(info.log.message, {
              payload:
                {
                  error: info
                }
            });
            break;
          default:
            logMessage(info.label + ": " + info.log.message);
            break;
        }
      }
    }

    function logMessage(msg) {
      console.log("kafka-publisher: " + node.name + " : " + msg);
    }

    RED.nodes.registerType("kafka-publisher", pushData);
}
