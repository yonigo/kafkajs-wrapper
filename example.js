const {consumer, producer} = require('./index');

const topicsList = ['example-topic'];
const groupId = 'my-test-group';

const onError = (e) => {
    console.log(`Error handling message ${e}`);
}

const onData = async (message) => {
    try {
        console.log(`Handling message ${message.value}`);
        await sleep(1000);
    }
    catch(e) {
        return Promise.reject(e);
    }
}


const run = async () => {
    try {
        await producer.start();
        await consumer.start({ groupId, topicsList, autoCommit: false, onData, onError });

        await producer.produce({topic: topicsList[0], message: {value: 'Test Message'}});
    }
    catch(e) {
        console.log(`An error occured ${e}`);
    }
}

const sleep = (ms) => {
    return new Promise(function (resolve, reject) {
        setTimeout(resolve, ms);
    });  
}

