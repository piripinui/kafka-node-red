const { Kafka } = require('kafkajs');

module.exports = function(RED) {
    function pushData(config) {
        RED.nodes.createNode(this, config);

        var node = this;

        node.kafkahost = config.kafkahost;
        node.kafkaport = config.kafkaport;
        node.kafkatopic = config.kafkatopic;
        node.kafkagroupId = config.kafkagroupId;

        var kafkaHost = node.kafkahost, kafkaPort = node.kafkaport, kafkaTopic = node.kafkatopic, groupId = node.kafkagroupId;

        log("Initialising on " + kafkaHost + ":" + kafkaPort);
        var kafka = new Kafka({
          clientId: 'kafka-consumer',
          brokers: [kafkaHost + ':' + kafkaPort]
        });
        var consumer = kafka.consumer({ groupId: groupId });

        consumer.connect()
        .catch(e => log(`${e.message}`, e));

        consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
        log("Listening to topic " + kafkaTopic);

        consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            var msg = {
              payload: JSON.parse(message.value)
            };

            try {
              log("Received message ", msg);
              node.send(msg);
            }
            catch(error) {
              node.error(error.message, msg);
            }
          }
        });
    }

    function log(msg) {
      console.log("kafka-consumer: " + msg);
    }

    RED.nodes.registerType("kafka-consumer", pushData);
}
