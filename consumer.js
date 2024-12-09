const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, SendMessageCommand } = require("@aws-sdk/client-sqs");
const Redis = require("ioredis");
const { AWS_REGION, MAIN_QUEUE_URL, DLQ_URL, REDIS_HOST, REDIS_PORT } = require("./config");

const sqs = new SQSClient({ region: AWS_REGION });
const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  tls: {}, // Enable TLS
});

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const processMessage = async (message) => {
  try {
    console.log("Received message:", message.Body);
    console.log(`Received MessageId: ${message.MessageId}, SentTimestamp: ${message.Attributes.SentTimestamp}`);

    const { clientId, transactionId, amount } = JSON.parse(message.Body);

    // Fetch client data from Redis
    const clientData = await redis.get(`clientId:${clientId}`);

    if (clientData) {
      const parsedClientData = JSON.parse(clientData);
      console.log("Client Data:", parsedClientData);
      console.log(`Transaction Details: ID=${transactionId}, Amount=${amount}`);
      
      // Simulate processing logic (20% failure rate for demonstration)
      const isProcessed = Math.random() > 0.2;

      if (isProcessed) {
        console.log("Message processed successfully.");
        return {
          success: true,
          clientData: parsedClientData,
          transactionId,
          amount
        };
      } else {
        console.error("Message processing failed. Retrying...");
        return { success: false };
      }
    } else {
      console.log(`No data found in Redis for clientId: ${clientId}`);
      return { success: false };
    }
  } catch (error) {
    console.error("Error processing message:", error);
    return { success: false };
  }
};

const processMessages = async () => {
  try {
    const command = new ReceiveMessageCommand({
      QueueUrl: MAIN_QUEUE_URL,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 5, // Long polling
      AttributeNames: ["All"], // Request all attributes            
    });

    const response = await sqs.send(command);

    if (response.Messages && response.Messages.length > 0) {
      for (const message of response.Messages) {
        // Get the retry count (ApproximateReceiveCount)
        const retryCount = message.Attributes?.ApproximateReceiveCount || 1;

        // Process the message
        const processResult = await processMessage(message);

        if (processResult.success) {
          
          // Delete the message from SQS on successful processing
          const deleteCommand = new DeleteMessageCommand({
            QueueUrl: MAIN_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          });
          await sqs.send(deleteCommand);
          console.log("Message deleted from SQS.");
        } else {
          // If retry count exceeds 3, move the message to DLQ
          if (retryCount >= 3) {
            const messageBody = message.Body;
            console.log(`Message failed 3 times. Moving to DLQ: ${messageBody}`);

            // Send the message to the Dead Letter Queue (DLQ)
            const reprocessCommand = new SendMessageCommand({
              QueueUrl: DLQ_URL,
              MessageBody: messageBody,
              MessageGroupId: "defaultGroup",
              MessageDeduplicationId: `${message.MessageId}-${Date.now()}`, // Modify ID for uniqueness 
            });
            await sqs.send(reprocessCommand);
            console.log("Message moved to DLQ.");

            // Delete from main queue as it reached max retries
            const deleteCommand = new DeleteMessageCommand({
              QueueUrl: MAIN_QUEUE_URL,
              ReceiptHandle: message.ReceiptHandle,
            });
            await sqs.send(deleteCommand);
            console.log("Message deleted from the main queue after 3 failed attempts.");
          }
        }
      }
    } else {
      console.log("No messages available.");
    }
  } catch (error) {
    console.error("Error processing messages:", error);
  }
};

const processDLQ = async () => {
  try {
    const receiveCommand = new ReceiveMessageCommand({
      QueueUrl: DLQ_URL,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 0,
    });

    const response = await sqs.send(receiveCommand);

    if (response.Messages && response.Messages.length > 0) {
      for (const message of response.Messages) {
        console.log("Received message from DLQ:", message.Body);

        // Reprocess the message by sending back to main queue
        const reprocessCommand = new SendMessageCommand({
          QueueUrl: MAIN_QUEUE_URL,
          MessageBody: message.Body,
          MessageGroupId: "defaultGroup",
          MessageDeduplicationId: `${message.MessageId}-${Date.now()}`, // Modify ID for uniqueness
        });
        await sqs.send(reprocessCommand);
        console.log("Message reprocessed and sent back to the main queue.");

        // Delete the message from the DLQ after reprocessing
        const deleteCommand = new DeleteMessageCommand({
          QueueUrl: DLQ_URL,
          ReceiptHandle: message.ReceiptHandle,
        });
        await sqs.send(deleteCommand);
        console.log("Message deleted from DLQ.");
      }
    } else {
      console.log("No messages in the DLQ.");
    }
  } catch (error) {
    console.error("Error processing DLQ:", error);
  }
};

// Poll Queues Continuously
const pollQueues = async () => {
  while (true) {
    console.log("\nPolling Main Queue...");
    await processMessages();

    console.log("\nPolling DLQ...");
    await processDLQ();

    // Delay before next polling
    await delay(2000); // 2 seconds
  }
};

// Start polling
pollQueues();
