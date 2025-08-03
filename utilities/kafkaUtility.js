import { KafkaJS } from "@confluentinc/kafka-javascript";

// Kafka Config
const KAFKA_BROKERS = 'localhost:9092';
export const kafka = new KafkaJS.Kafka({ brokers: [KAFKA_BROKERS] });

export async function syncHandler({ groupId, topic, syncFunction }) {
  const consumer = kafka.consumer({ groupId });

  await consumer.connect();
  await consumer.subscribe({ topic });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(`[Kafka: ${topic}]: New message received: ${message.value.toString()}`);
      const event = JSON.parse(message.value.toString());
      // const { userId, username } = event;
  
      try{
        const result = await syncFunction(event);
        console.log(`[Kafka: ${topic}]: User synced: ${result}`);
      } catch(err) {
        console.log(`[Kafka: ${topic}]: Error syncing user: ${err}`);
      }
    }
  });
}