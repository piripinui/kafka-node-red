const { Kafka } = require('kafkajs');

module.exports = function(RED) {
    function pushData(config) {
        RED.nodes.createNode(this, config);

        var node = this;

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

        log("Initialising on " + kafkaHost + ":" + kafkaPort);
        var kafka = new Kafka({
          clientId: 'kafka-publisher',
          brokers: [kafkaHost + ':' + kafkaPort],
          connectionTimeout: kafkaConnectionTimeout,
          requestTimeout: kafkaRequestTimeout
        });
        var producer = kafka.producer();

        producer.connect()
        .catch(e => console.error(`[example/producer] ${e.message}`, e));

        node.on('input', function(msg, send, done) {
            log("Sending message to " + kafkaTopic);

            producer.send({
              topic: kafkaTopic,
              messages: [
                { value: JSON.stringify(msg.payload) },
              ]
            })
            .then(log)
            .catch(e => {
              node.error(e.message, msg);
              if (done) {
                  done();
              }
            });

            if (done) {
                done();
            }
        });
    }

    function log(msg) {
      console.log("kafka-publisher: " + msg);
    }

    RED.nodes.registerType("kafka-publisher", pushData);
}
