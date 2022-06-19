const { Kafka } = require('kafkajs')
 

async function setupKafka ({
    groupId,
    topics
}) {
    const kafka = new Kafka({
        brokers: [process.env.KAFKA_URL],
        sasl: {
          mechanism: 'scram-sha-256',
          username: process.env.KAFKA_USERNAME,
          password: process.env.KAFKA_PASSWORD
        },
        ssl: true,
    })
    const consumer = kafka.consumer({ groupId })
    await consumer.connect()
    await consumer.subscribe({ topics, fromBeginning: true })
    return consumer;
};

module.exports = { setupKafka };