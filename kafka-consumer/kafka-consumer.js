const { Kafka } = require('kafkajs'),
createLogger = require('./logger.js');

module.exports = function(RED) {
    function pushData(config) {
        RED.nodes.createNode(this, config);

        let node = this;

        let loggerConfig = {
          logger: {
            name: process.env.LOGGER_NAME || 'kafka-consumer: ' + node.name,
            level: process.env.LOGGER_LEVEL || 'info',
          }
        },
        logger = createLogger(loggerConfig.logger);

        const errorReporterCreator = logLevel =>  {
          return function(info) {

            switch(info.label) {
              case 'ERROR':
                logger.error(info.label + ": " + info.log.message);

                node.error(info.log.message, {
                  payload:
                    {
                      error: info
                    }
                });
                break;
              default:
                logger.info(info.label + ": " + info.log.message);
                break;
            }
          }
        }

        node.kafkahost = config.kafkahost;
        node.kafkaport = config.kafkaport;
        node.kafkatopic = config.kafkatopic;
        node.kafkagroupId = config.kafkagroupId;
        node.kafkaconnectiontimeout = config.kafkaconnectiontimeout;
        node.kafkarequesttimeout = config.kafkarequesttimeout;

        let kafkaHost = node.kafkahost,
        kafkaPort = node.kafkaport,
        kafkaTopic = node.kafkatopic,
        groupId = node.kafkagroupId,
        kafkaConnectionTimeout = node.kafkaconnectiontimeout,
        kafkaRequestTimeout = node.kafkarequesttimeout;

        logger.info("Initialising on " + kafkaHost + ":" + kafkaPort);
        let kafka = new Kafka({
          clientId: 'kafka-consumer',
          brokers: [kafkaHost + ':' + kafkaPort],
          connectionTimeout: kafkaConnectionTimeout,
          requestTimeout: kafkaRequestTimeout,
          logCreator: errorReporterCreator
        });
        let consumer = kafka.consumer({ groupId: groupId });

        const run = async() => {
          await consumer.connect()

          await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
          logger.info("Listening to topic " + kafkaTopic);

          await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
              // Message should have a property called value containing a JSON string.

              let msg = {
                payload: JSON.parse(message.value.toString())
              };

              try {
                node.send(msg);
                logger.info("Received response, sent message: %s", msg);
              }
              catch(error) {
                logger.error("Problem sending message to node: %s", error.message);
                node.error(error.message, msg);
              }
            }
          })
        }

        node.on('close', function() {
          consumer.disconnect();
          logger.info("Disconnecting from Kafka.");
        });

        run().catch(errorReporterCreator);
    }

    RED.nodes.registerType("kafka-consumer", pushData);
}
