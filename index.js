const express = require('express');
const { Kafka } = require('kafkajs');
require('dotenv').config()
const app = express();
const port = 3000;

let consumer;
let producer;

const kafka = new Kafka({
    clientId: 'kafka-test',
    brokers: ['glider-02.srvs.cloudkafka.com:9094', 'glider-03.srvs.cloudkafka.com:9094', 'glider-01.srvs.cloudkafka.com:9094'],
    ssl: true,
    sasl: {
        mechanism: 'scram-sha-256', // scram-sha-256 or scram-sha-512
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
    },
})

app.get('/send', async (req, res) => {
    producer = kafka.producer()

    await producer.connect()
    await producer.send({
        topic: '9varxaec-test',
        messages: [
            { value: 'Hello KafkaJS user!' },
        ],
    })
    res.send('Sended message to Kafka consumer!')
})

app.get('/get', async (req, res) => {
    consumer = kafka.consumer({ groupId: 'test-group' });

    let values = [];

    await consumer.connect();
    await consumer.subscribe({ topic: '9varxaec-test', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log( message.value.toString());
        },
    })
    res.send('Get message to Kafka consumer!');
})

app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})

process.once('SIGINT', async function (code) {
    await producer?.disconnect();
});