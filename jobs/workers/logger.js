const { Worker, QueueScheduler } = require('bullmq');
const { renderRedis } = require('../../connection'); 
const queueName = 'LOGGER';
async function setupBullMQProcessor() {
    const queueScheduler = new QueueScheduler(queueName, { connection: renderRedis });
    await queueScheduler.waitUntilReady();
  
    new Worker(queueName, async (job) => {
      for (let i = 0; i <= 100; i++) {
        await sleep(Math.random());
        // await job.updateProgress(i);
        await job.log(`Processing job at interval ${i}`);
        console.log('job data', job.data);
  
        if (Math.random() * 200 < 1) throw new Error(`Random error ${i}`);
      }
  
      return { jobId: `This is the return value of job (${job.id})` };
    }, { connection: renderRedis });
}

module.exports = { queueName, setupBullMQProcessor };