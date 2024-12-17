const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, SendMessageCommand } = require("@aws-sdk/client-sqs");
const Redis = require("ioredis");
const { AWS_REGION, MAIN_QUEUE_URL, DLQ_URL, REDIS_HOST, REDIS_PORT } = require("./config");

const sqs = new SQSClient({ region: AWS_REGION });
const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  tls: {}, // Enable TLS
});

// Helper to simulate delay
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Process a single message
const processMessage = async (message) => {
  try {
    console.log("Processing message:", message.Body);
    const { clientId } = JSON.parse(message.Body);

    // Fetch client data from Redis
    const clientData = await redis.get(clientId);

    if (clientData) {
      const parsedClientData = JSON.parse(clientData);
      console.log("Client Data Found:", parsedClientData);
      return { success: true };
    } else {
      console.error(`No data found for clientId: ${clientId}`);
      return { success: false };
    }
  } catch (error) {
    console.error("Error processing message:", error);
    return { success: false };
  }
};

// Process messages from Main Queue
const processMainQueue = async () => {
  try {
    const command = new ReceiveMessageCommand({
      QueueUrl: MAIN_QUEUE_URL,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 5,
      AttributeNames: ["All"],
    });

    const response = await sqs.send(command);

    if (response.Messages && response.Messages.length > 0) {
      for (const message of response.Messages) {
        const retryCount = message.Attributes?.ApproximateReceiveCount || 1;
        const processResult = await processMessage(message);

        if (processResult.success) {
          const deleteCommand = new DeleteMessageCommand({
            QueueUrl: MAIN_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          });
          await sqs.send(deleteCommand);
          console.log("Message successfully processed and deleted from Main Queue.");
        } else if (retryCount >= 3) {
          const dlqCommand = new SendMessageCommand({
            QueueUrl: DLQ_URL,
            MessageBody: message.Body,
            MessageGroupId: "defaultGroup",
            MessageDeduplicationId: `${message.MessageId}-${Date.now()}`,
          });
          await sqs.send(dlqCommand);

          const deleteCommand = new DeleteMessageCommand({
            QueueUrl: MAIN_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          });
          await sqs.send(deleteCommand);
          console.log("Message moved to DLQ after 3 retries.");
        }
      }
    } else {
      console.log("No messages in Main Queue.");
    }
  } catch (error) {
    console.error("Error processing Main Queue:", error);
  }
};

// Process messages from DLQ
const processDLQ = async () => {
  try {
    const command = new ReceiveMessageCommand({
      QueueUrl: DLQ_URL,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 5,
    });

    const response = await sqs.send(command);

    if (response.Messages && response.Messages.length > 0) {
      for (const message of response.Messages) {
        console.log("Reprocessing message from DLQ:", message.Body);

        const reprocessCommand = new SendMessageCommand({
          QueueUrl: MAIN_QUEUE_URL,
          MessageBody: message.Body,
          MessageGroupId: "defaultGroup",
          MessageDeduplicationId: `${message.MessageId}-${Date.now()}`,
        });
        await sqs.send(reprocessCommand);

        const deleteCommand = new DeleteMessageCommand({
          QueueUrl: DLQ_URL,
          ReceiptHandle: message.ReceiptHandle,
        });
        await sqs.send(deleteCommand);

        console.log("Message successfully reprocessed and deleted from DLQ.");
      }
    } else {
      console.log("No messages in DLQ.");
    }
  } catch (error) {
    console.error("Error processing DLQ:", error);
  }
};

// Run Main Queue and DLQ Processing in Parallel
const pollQueues = async () => {
  while (true) {
    await Promise.allSettled([processMainQueue(), processDLQ()]);
    console.log("Polling completed. Waiting for the next cycle...");
    //await delay(5000); // Add delay before polling again
  }
};

// Start polling
pollQueues();
