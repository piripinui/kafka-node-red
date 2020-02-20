const { Kafka } = require('kafkajs');

module.exports = function(RED) {
    function pushData(config) {
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

        RED.nodes.createNode(this, config);

        let node = this;

        node.kafkahost = config.kafkahost;
        node.kafkaport = config.kafkaport;
        node.kafkatopic = config.kafkatopic;
        node.kafkareplytopic = config.kafkareplytopic;
        node.kafkaconnectiontimeout = config.kafkaconnectiontimeout;
        node.kafkarequesttimeout = config.kafkarequesttimeout;

        let kafkaHost = node.kafkahost,
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
        let producer = kafka.producer();

        const run = async() => {
          await producer.connect()

          node.on('input', function(msg, send, done) {
              logMessage("Reply topic = ", node.kafkareplytopic);
              if (typeof node.kafkareplytopic == "undefined") {
                logMessage("Sending message to " + kafkaTopic);
                producer.send({
                  topic: kafkaTopic,
                  messages: [
                    { value: JSON.stringify(msg.payload) }
                  ]
                })
              }
              else {
                producer.send({
                  topic: kafkaTopic,
                  messages: [
                    {
                      value: JSON.stringify(msg.payload),
                      headers: {
                        'kafka_replyTopic': node.kafkareplytopic
                      }
                    }
                  ]
                })

                logMessage("Sent message to " + kafkaTopic + " - set reply topic to " + node.kafkareplytopic);
              }
          });
        }

        run().catch(errorReporterCreator);
    }

    RED.nodes.registerType("kafka-publisher", pushData);
}
