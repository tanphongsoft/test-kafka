const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const { Queue: QueueMQ, Worker, QueueScheduler } = require('bullmq');
const session = require('express-session');
const bodyParser = require('body-parser');
const passport = require('passport');
const LocalStrategy = require('passport-local').Strategy;
const { ensureLoggedIn } = require('connect-ensure-login');
const express = require('express');
const { setupKafka } = require('./setup-kafka');
const { renderRedis } = require('./connection');
// Configure the local strategy for use by Passport.
//
// The local strategy require a `verify` function which receives the credentials
// (`username` and `password`) submitted by the user.  The function must verify
// that the password is correct and then invoke `cb` with a user object, which
// will be set at `req.user` in route handlers after authentication.
passport.use(
  new LocalStrategy(function (username, password, cb) {
    if (!username || !password) return cb(null, false);
    if (username === process.env.ADMIN_USER && password === process.env.ADMIN_PASSWORD) {
      return cb(null, { user: 'bull-board' });
    }
    return cb(null, false);
  })
);

// Configure Passport authenticated session persistence.
//
// In order to restore authentication state across HTTP requests, Passport needs
// to serialize users into and deserialize users out of the session.  The
// typical implementation of this is as simple as supplying the user ID when
// serializing, and querying the user record by ID from the database when
// deserializing.
passport.serializeUser((user, cb) => {
  cb(null, user);
});

passport.deserializeUser((user, cb) => {
  cb(null, user);
});

const sleep = (t) => new Promise((resolve) => setTimeout(resolve, t * 1000));

const createQueueMQ = (name) => new QueueMQ(name, { connection: renderRedis });

async function setupBullMQProcessor(queueName) {
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

const run = async () => {
  const consumer = await setupKafka({ groupId: 'test-kafka', topics: ['session', 'user', 'property']})
  const exampleBullMq = createQueueMQ('ExampleBullMQ');

  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath('/ui');

  createBullBoard({
    queues: [new BullMQAdapter(exampleBullMq)],
    serverAdapter,
  });

  await setupBullMQProcessor(exampleBullMq.name);
  const app = express();
  // Configure view engine to render EJS templates.
  app.set('views', __dirname + '/views');
  app.set('view engine', 'ejs');
  console.log('abc');
  app.use(session({ secret: 'keyboard cat', saveUninitialized: true, resave: true }));
  app.use(bodyParser.urlencoded({ extended: false }));
  console.log('abc');
  // Initialize Passport and restore authentication state, if any, from the session.
  app.use(passport.initialize({}));
  app.use(passport.session({}));
  console.log('abc');
  app.get('/ui/login', (req, res) => {
    res.render('login', { invalid: req.query.invalid === 'true' });
  });

  app.post(
    '/ui/login',
    passport.authenticate('local', { failureRedirect: '/ui/login?invalid=true' }),
    (req, res) => {
      res.redirect('/ui');
    }
  );

  app.use('/add', (req, res) => {
    const opts = req.query.opts || {};

    if (opts.delay) {
      opts.delay = +opts.delay * 1000; // delay must be a number
    }

    exampleBullMq.add('Add', { title: req.query.title }, opts);

    res.json({
      ok: true,
    });
  });
  app.use('/healthcheck', (req, res) => {
    res.json({ status: 'ok' })
  })
  app.use('/ui', ensureLoggedIn({ redirectTo: '/ui/login' }), serverAdapter.getRouter());
  
  await consumer.run({
    eachBatch: async (eachBatchPayload) => {
      const { topic, partition, batch } = eachBatchPayload
      for (const message of batch.messages) {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
        console.log(`- ${prefix} ${message.key}#${message.value}`) 
        exampleBullMq.add(prefix, { message: JSON.parse(message.value.toString()) });
      }
    }
  })
  
  app.listen(process.env.PORT || 3002, () => {
    console.log('Running on 3000...');
    console.log('For the UI, open http://localhost:3000/ui');
    console.log('Make sure Redis is running on port 6379 by default');
    console.log('To populate the queue, run:');
    console.log('  curl http://localhost:3000/add?title=Example');
    console.log('To populate the queue with custom options (opts), run:');
    console.log('  curl http://localhost:3000/add?title=Test&opts[delay]=9');
  });
};

// eslint-disable-next-line no-console
run().catch((e) => console.error(e));