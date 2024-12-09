const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { AWS_REGION, MAIN_QUEUE_URL } = require("./config");

const sqs = new SQSClient({ region: AWS_REGION });

const sendTransaction = async (transactionId, amount, clientId) => {
  try {
    const messageBody = JSON.stringify({ transactionId, amount, clientId });

    const command = new SendMessageCommand({
      QueueUrl: MAIN_QUEUE_URL,
      MessageBody: messageBody,
      MessageGroupId: "messageGroup",
      MessageDeduplicationId: `${transactionId}-${Date.now()}`,


    });

    const response = await sqs.send(command);
    console.log("Message sent successfully:", response.MessageId);

  } catch (error) {
    console.error("Error sending message:", error);
  }
};

// Example usage
const sendTransactionsSequentially = async () => {
  await sendTransaction("txn001", 500, "client123");
  await sendTransaction("txn002", 300, "client456");
  await sendTransaction("txn003", 400, "client789");
};

sendTransactionsSequentially();
