const { Kafka } = require('kafkajs'),
createLogger = require('./logger.js');

module.exports = function(RED) {
    function pushData(config) {
      RED.nodes.createNode(this, config);

      let node = this;

      let loggerConfig = {
        logger: {
          name: process.env.LOGGER_NAME || 'kafka-publisher: ' + node.name,
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
        node.kafkareplytopic = config.kafkareplytopic;
        node.kafkaconnectiontimeout = config.kafkaconnectiontimeout;
        node.kafkarequesttimeout = config.kafkarequesttimeout;

        let kafkaHost = node.kafkahost,
        kafkaPort = node.kafkaport,
        kafkaTopic = node.kafkatopic,
        kafkaConnectionTimeout = node.kafkaconnectiontimeout,
        kafkaRequestTimeout = node.kafkarequesttimeout;

        logger.info("Initialising on " + kafkaHost + ":" + kafkaPort);
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
              logger.info("Reply topic = <%s>", node.kafkareplytopic);
              if (typeof node.kafkareplytopic == "undefined") {
                logger.info("Sending message to %s", kafkaTopic);
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

                logger.info("Sent message to %s - set reply topic to <%s>", kafkaTopic, node.kafkareplytopic);
              }
          });
        }

        run().catch(errorReporterCreator);
    }

    RED.nodes.registerType("kafka-publisher", pushData);
}
