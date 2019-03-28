const logger = require('@insights/logger');
const COMMIT_TIME_INTERVAL = 5000;

class CommitManager {

    constructor() {
        this.partitionsData = {};
        this.partitionCallbacks = {};
        this.lastCommited = [];
    }

    start(kafkaConsumer, config) {
        this.kafkaConsumer = kafkaConsumer;
        this.commitInterval = config.commitInterval || COMMIT_TIME_INTERVAL;
        if (!config.autoCommit) {
            setInterval(() => {
                this.commitProcessedOffsets();
            }, COMMIT_TIME_INTERVAL)
        }

        this.kafkaConsumer.on(this.kafkaConsumer.events.COMMIT_OFFSETS, (data) => {
            logger.debug(`Commit  ${JSON.stringify(data)}`)
        })
    }

    notifyStartProcessing(data) {
        const partition = data.partition;
        const offset = data.offset;
        const topic = data.topic;
        this.partitionsData[partition] = this.partitionsData[partition] || [];
        this.partitionsData[partition].push({
            offset: offset,
            topic: topic,
            done: false
        });
    }

    notifyFinishedProcessing(data) {
        const partition = data.partition;
        const offset = data.offset;
        this.partitionsData[partition] = this.partitionsData[partition] || [];
        let record = this.partitionsData[partition].filter((record) => { return record.offset === offset })[0];
        if (record) {
            record.done = true;
        }
    }

    async commitProcessedOffsets() {
        try {
            let offsetsToCommit = [];
            for (let key in this.partitionsData) {
                const partition = key - 0;
                await this.partitionCallbacks[partition].heartbeat();
                let pi = this.partitionsData[key].findIndex((record) => { return record.done }); // last processed index
                let npi = this.partitionsData[key].findIndex((record) => { return !record.done }); // first unprocessed index
                let lastProcessedRecord = npi > 0 ?
                    this.partitionsData[key][npi - 1] : (pi > -1 ? this.partitionsData[key][this.partitionsData[key].length - 1] : null);
                if (lastProcessedRecord) {
                    if (!this.partitionCallbacks[partition].isRunning()) break;
                    await this.partitionCallbacks[partition].resolveOffset(lastProcessedRecord.offset);
                    await this.partitionCallbacks[partition].commitOffsetsIfNecessary();
                    this.partitionsData[key].splice(0, this.partitionsData[key].indexOf(lastProcessedRecord) + 1); // remove commited records from array
                    offsetsToCommit.push({ partition: key - 0, offset: lastProcessedRecord.offset, topic: lastProcessedRecord.topic });
                }
            }

            this.lastCommited = offsetsToCommit.length > 0 ? offsetsToCommit : this.lastCommited;
            Promise.resolve();

        }
        catch (e) {
            Promise.reject(e);
        }
    }

    setPartitionCBs({ partition, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning }) {
        this.partitionCallbacks[partition] = { resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning }
    }

    getLastCommited() {
        return this.lastCommited;
    }
}

module.exports = new CommitManager();