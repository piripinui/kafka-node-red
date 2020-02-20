const bunyan = require('bunyan');

function createLogger(logConfig) {
    return bunyan.createLogger({
        ...logConfig,
    });
}

module.exports = createLogger;
