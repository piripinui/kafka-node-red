const { Kafka } = require('kafkajs');

module.exports = function(RED) {
    var node;

    function pushData(config) {
        RED.nodes.createNode(this, config);

        node = this;

        node.kafkahost = config.kafkahost;
        node.kafkaport = config.kafkaport;
        node.kafkatopic = config.kafkatopic;
        node.kafkagroupId = config.kafkagroupId;
        node.kafkaconnectiontimeout = config.kafkaconnectiontimeout;
        node.kafkarequesttimeout = config.kafkarequesttimeout;

        var kafkaHost = node.kafkahost,
        kafkaPort = node.kafkaport,
        kafkaTopic = node.kafkatopic,
        groupId = node.kafkagroupId,
        kafkaConnectionTimeout = node.kafkaconnectiontimeout,
        kafkaRequestTimeout = node.kafkarequesttimeout;

        logMessage("Initialising on " + kafkaHost + ":" + kafkaPort);
        var kafka = new Kafka({
          clientId: 'kafka-consumer',
          brokers: [kafkaHost + ':' + kafkaPort],
          connectionTimeout: kafkaConnectionTimeout,
          requestTimeout: kafkaRequestTimeout,
          logCreator: errorReporterCreator
        });
        var consumer = kafka.consumer({ groupId: groupId });

        consumer.connect()

        consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
        logMessage("Listening to topic " + kafkaTopic);

        consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            var msg = {
              payload: JSON.parse(message.value)
            };

            try {
              logMessage("Received message ", msg);
              node.send(msg);
            }
            catch(error) {
              node.error(error.message, msg);
            }
          }
        })
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
      console.log("kafka-consumer: " + node.name + " : " + msg);
    }

    RED.nodes.registerType("kafka-consumer", pushData);
}
