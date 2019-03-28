const { Kafka } = require('kafkajs');
const config = require('./config');

class Client {

    constructor() {
        const clientConfig = {
            clientId: config.clientId,
            brokers: config.brokerList.split(','),
            logLevel: config.loggerLevel
        }
        this.kafkaClient = new Kafka(clientConfig);
    }

}

// Singleton client instance
module.exports = (new Client()).kafkaClient;