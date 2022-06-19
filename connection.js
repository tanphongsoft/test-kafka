const Redis = require("ioredis");

// Internal Redis URL, extract the details into environment variables.
// "redis://red-xxxxxxxxxxxxxxxxxxxx:6379"

const renderRedis = new Redis({
  host: process.env.REDIS_SERVICE_NAME, // Render Redis service name, red-xxxxxxxxxxxxxxxxxxxx
  port: process.env.REDIS_PORT || 6379, // Redis port
});

module.exports = { renderRedis }