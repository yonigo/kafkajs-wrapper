const kafkaClient = require('./client');
const logger = require('./logger');
const config = require('./config');

class Producer {

    constructor() {
        this.ready = false;
        this.kafkaProducer;
    }

    async start() {
        try {
            if (this.ready)
                return Promise.resolve();

            this.kafkaProducer = kafkaClient.producer();
            this.kafkaProducer.connect();
            logger.info(`Kafka Producer is Ready`);
            this.ready = true;
        }
        catch (e) {
            return Promise.reject(e);
        }
    }

    async produce({ topic, messages }) {
        try {
            if (!this.ready) {
                logger.error('Producer not ready');
                throw ('Producer not ready');
            }

            if (!Array.isArray(messages)) {
                messages = [messages];
            }

            if (messages.length == 0)
                return Promise.resolve();

            messages = messages.map((m => {
                const { value, headers, key } = m;
                return { value, headers, key };
            }))

            await this.kafkaProducer.send({
                acks: config.acks,
                topic,
                messages
            });

            logger.debug(`Producer sent message msg to topic ${topic}`);
            return Promise.resolve();
        }
        catch (err) {
            logger.error(`A problem occurred when sending our message: ${err}`);
            return Promise.reject(err)
        }
    }

    produceBulkMsg({ topic, messages }) {
        return this.produce({ topic, messages });
    }

}

module.exports = new Producer();

