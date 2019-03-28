const async = require('async');
const kafkaClient = require('./client');
const config = require('./config');
const logger = require('/logger');
const kafkaProducer = require('./producer');
const commitManager = require('./commitManager');

class Consumer {

  constructor() {

    this.ready = false;
    this.paused = false;
    this.kafkaConsumer;
    if (config.maxParallelHandles) {

      this.msgQueue = async.queue(async (data, done) => {
        await this.handleCB(data, this.onData);
        done();
      }, config.maxParallelHandles);
      this.msgQueue.drain = async () => {
        if (this.paused) 
          this.retryResume();
      }
    }
  }

  async start({ groupId, topicsList, onData, onError, autoCommit }) {
    try {

      if (this.ready)
        return Promise.resolve();

      if (!topicsList)
        return Promise.reject('Cannot start without a topic list');

      this.topicsList = topicsList;
      this.onData = onData || this.onData;
      this.onError = onError || this.onError;
      this.autoCommit = autoCommit || false;

      const consumerConfig = {
        groupId: groupId || config.DEFAULT_GROUP_ID,
        maxBytesPerPartition: config.maxBytesPerPartition,
        heartbeatInterval: config.heartbeatInterval,
        fromBeginning: config.fromBeginning
      }

      this.kafkaConsumer = kafkaClient.consumer(consumerConfig);
      this.kafkaConsumer.connect();
      topicsList.forEach((t) => {
        this.kafkaConsumer.subscribe({ topic: t });
      });

      commitManager.start(this.kafkaConsumer, {});
      this.ready = true;
      logger.info('Kafka consumer ready');

      const onMessageBatch = async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning }) => {
        commitManager.setPartitionCBs({ partition: batch.partition, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning });
        for (let message of batch.messages) {
          message.partition = batch.partition;
          message.topic = batch.topic;
          logger.debug(`message received from kafka,partition: ${message.partition}, offset: ${message.offset}`);
          if (config.maxParallelHandles) {

            this.msgQueue.push(message);
            if (this.msgQueue.length() > config.maxQueueSize && !this.paused) {

              try {
                this.kafkaConsumer.pause(this.topicsList.map((t) => { return { topic: t } }));
              }
              catch (e) {
                logger.error("Pause err", e);
              }
              this.paused = true;

            }
          } else 
            this.handleCB(message, this.onData);
        }
      }

      await this.kafkaConsumer.run({
        eachBatch: onMessageBatch
      });

    }
    catch (e) {
      return Promise.reject(e);
    }
  }

  async handleCB(data, handler) {
    try {
      try {
        commitManager.notifyStartProcessing(data);
        await handler(data);
      }
      catch (e) {
        logger.error(`Error handling message: ${e}`);
        data.headers = data.headers || {};
        data.headers.originalTopic = data.topic;
        await kafkaProducer.produce({
          topic: config.RETRY_TOPIC,
          messages: data
        });
      }
    }
    catch (e) {
      logger.error(`Error producing to retry: ${e}`);
    }
    finally {
      commitManager.notifyFinishedProcessing(data);
    }
  }

  async onData(data) {
    logger.debug(`Handling received message with offset: ${data.offset}`);
    return Promise.resolve();
  }

  // Sometimes resume fails due to rebalance. we need to retry until success
  retryResume() {
    const MAX_RETRIES = 5;
    let tryNum = 0;
    const interval = setInterval(helper.bind(this), 500);
    helper.call(this);

    async function helper() {

      tryNum++;
      if (tryNum > MAX_RETRIES) {
        logger.error('Unable to resume consumption');
        process.kill(process.pid);
      }

      if (this.paused) {
        try {
          if (!this.autoCommit)
            await commitManager.commitProcessedOffsets(true);
          this.kafkaConsumer.resume(this.topicsList.map((t) => { return { topic: t } }));
          this.paused = false;
          logger.debug(`Resume successful for ${JSON.stringify(this.topicsList)}`);
          clearInterval(interval);
        }
        catch (e) {
          logger.error(`Resume err ${e}`);
        }
      } else {
        clearInterval(interval);
      }
    }
  }

}

module.exports = new Consumer();
