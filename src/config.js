const logger = require('./logger');

const config = {
    brokerList: process.env.KAFKA_BROKERS_LIST || 'broker:9092',
    maxBytesPerPartition: process.env.FETCH_MESSAGE_MAX_BYTES - 0 || 10485760,
    maxParallelHandles: process.env.MAX_PARALLEL_HANDLES - 0 || 2000,
    maxQueueSize: process.env.MAX_QUEUE_SIZE - 0 || 5000,
    retryTopic: process.env.RETRY_TOPIC || 'retry',
    heartbeatInterval: process.env.HEARTBEAT_INTERVAL - 0 ||  6000,
    commitInterval: process.env.COMMIT_INTERVAL - 0 ||  5000,
    loggerLevel : process.env.LOGGER_LEVEL || 'info',
    defaultGroupId: process.env.DEFAULT_GROUP_ID || 'kafka-consumer-group',
    fromBeginning: process.env.FROM_BEGINNING || true,
    acks: process.env.ACKS || 1

}

logger.debug(`Config: ${JSON.stringify(config)}`);

module.exports = config;