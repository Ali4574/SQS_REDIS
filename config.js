require("dotenv").config();

module.exports = {
  AWS_REGION: process.env.AWS_REGION,
  MAIN_QUEUE_URL: process.env.MAIN_QUEUE_URL,
  DLQ_URL: process.env.DLQ_URL,
  REDIS_HOST: process.env.REDIS_HOST,
  REDIS_PORT: parseInt(process.env.REDIS_PORT, 10),
};
