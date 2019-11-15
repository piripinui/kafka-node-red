# node-red-addons

Provides various additional node types for [Node-RED](https://nodered.org/).

## kafka-publisher and kafka-consumer

Provides Kafka publisher and consumer node types for [Node-RED](https://nodered.org/) using [kafkajs](https://www.npmjs.com/package/kafkajs).
### Installation
1. Clone or copy this repo
2. Install the nodes using the following instructions - https://nodered.org/docs/user-guide/runtime/adding-nodes. This means executing commands something like this inside the .node-red directory:

    `npm install <repo-dir>/kafka-publisher`
    `npm install <repo-dir>/kafka-consumer`

3. Start Node-RED

## uuid-injector

This node creates a UUID and injects into the received message in the specified location using a JSON path string defined in the node's editor inside Node-RED.
