const uuidv1 = require('uuid/v1'),
jsonpath = require('jsonpath');

module.exports = function(RED) {
    function injectUUID(config) {
        RED.nodes.createNode(this, config);
        var node = this;

        node.injectionpath = config.injectionpath;

        this.on('input', function(msg, send, done) {
            try {
              var myUUID = uuidv1();

              var results = jsonpath.value(msg.payload, node.injectionpath, myUUID);

              node.send(msg);

              if (done) {
                  done();
              }
            }
            catch(err) {
              if (done) {
                  // Node-RED 1.0 compatible
                  done(err);
              } else {
                  // Node-RED 0.x compatible
                  node.error(err, msg);
              }
            }
        });
    }
    RED.nodes.registerType("uuid-injector", injectUUID);
}
