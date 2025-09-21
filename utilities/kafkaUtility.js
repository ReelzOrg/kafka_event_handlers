// import { KafkaJS } from "@confluentinc/kafka-javascript";

// // Kafka Config
// const KAFKA_BROKERS = 'localhost:9092';
// export const kafka = new KafkaJS.Kafka({ brokers: [KAFKA_BROKERS] });

// export async function syncHandler({ groupId, topic, syncFunction }) {
//   const consumer = kafka.consumer({ groupId });

//   await consumer.connect();
//   await consumer.subscribe({ topic });

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       console.log(`[Kafka: ${topic}]: New message received: ${message.value.toString()}`);
//       const event = JSON.parse(message.value.toString());
//       // const { userId, username } = event;
  
//       try{
//         const result = await syncFunction(event);
//         console.log(`[Kafka: ${topic}]: Synced: ${result}`);
//       } catch(err) {
//         console.log(`[Kafka: ${topic}]: Error syncing: ${err}`);
//       }
//     }
//   });
// }
import { KafkaJS } from "@confluentinc/kafka-javascript";

// Kafka Config (prefer environment variables in production)
// e.g., KAFKA_BROKERS="broker1:9092,broker2:9092" and parse into array
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map(b => b.trim())
  .filter(Boolean);
const kafka = new KafkaJS.Kafka({ 
  // Note: Kafka client expects an array of broker strings
  brokers: KAFKA_BROKERS,
  retry: { // Add Kafka-level retries for broker communication
    initialRetryTime: 300,
    retries: 5
  }
});

/**
 * @class KafkaConsumerManager
 * Manages a single Kafka consumer for a given group ID, handling multiple topic subscriptions,
 * message processing with retries, a dead-letter queue, and graceful shutdown.
 * @constructor {groupId, options}
 * @param {string} groupId - The group ID for the consumer.
 * @param {Object} options - The options for the consumer (default values in the env file)
 */
export class KafkaConsumerManager {
  constructor(groupId, options = {}) {
    this.consumer = kafka.consumer({ 
      groupId,
      // Allow more time for processing, especially with retries
      sessionTimeout: 30000,
      // Heartbeat interval should be lower than sessionTimeout
      heartbeatInterval: 3000
    });
    this.producer = kafka.producer(); // For DLQ
    this.topicHandlers = new Map();
    this.groupId = groupId;
    // Tuning options (can be overridden via constructor or env)
    this.maxAttempts = options.maxAttempts || Number(process.env.KAFKA_HANDLER_MAX_ATTEMPTS || 3);
    this.backoffMs = options.backoffMs || Number(process.env.KAFKA_HANDLER_BACKOFF_MS || 250);
    this.maxBatchSize = options.maxBatchSize || Number(process.env.KAFKA_MAX_BATCH_SIZE || 100);
    this.maxBatchTimeMs = options.maxBatchTimeMs || Number(process.env.KAFKA_MAX_BATCH_TIME_MS || 500);
  }

  /**
   * Registers a handler function for a specific topic.
   * @param {string} topic - The topic to subscribe to.
   * @param {Function} handler - The async function to process messages from the topic.
   */
  registerHandler(topic, handler) {
    if (this.topicHandlers.has(topic)) {
      console.warn(`[KafkaConsumerManager] Handler for topic "${topic}" is being overridden.`);
    }
    this.topicHandlers.set(topic, handler);
  }

  /**
   * Connects the consumer and producer, subscribes to registered topics, and starts processing messages.
   */
  async start() {
    try {
      await this.consumer.connect();
      await this.producer.connect();

      const topics = Array.from(this.topicHandlers.keys());
      if (topics.length === 0) {
        console.warn(`[KafkaConsumerManager] No topic handlers registered for group "${this.groupId}". Consumer will not start.`);
        return;
      }

      // KafkaJS requires subscribing per topic (no multi-topic option in a single call)
      for (const topic of topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }

      console.log(`[KafkaConsumerManager] Consumer for group "${this.groupId}" started, subscribed to topics: ${topics.join(', ')}`);

      await this.consumer.run({
        // Use batch processing for better throughput and manual offset control
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
          const handler = this.topicHandlers.get(batch.topic);
          if (!handler) {
            // No handler registered; resolve all offsets to avoid blocking
            for (const message of batch.messages) {
              resolveOffset(message.offset);
            }
            await commitOffsetsIfNecessary();
            return;
          }

          for (const message of batch.messages) {
            if (!isRunning() || isStale()) break;
            let event;
            try {
              event = JSON.parse(message.value.toString());
            } catch (parseErr) {
              console.error(`[Kafka: ${batch.topic}]: Failed to parse message, sending to DLQ`, {
                error: parseErr.message,
                raw: message.value?.toString()
              });
              await this.sendToDLQ(batch.topic, message, parseErr, { attempts: 0, partition: batch.partition });
              resolveOffset(message.offset);
              continue;
            }

            const attemptResult = await this.processWithRetry(() => handler(event), this.maxAttempts, this.backoffMs);

            if (!attemptResult.ok) {
              await this.sendToDLQ(batch.topic, message, attemptResult.error, { attempts: attemptResult.attempts, partition: batch.partition });
            }

            // Mark this message as processed regardless (DLQ on failure)
            resolveOffset(message.offset);

            // Heartbeat is necessary to keep the consumer session alive. The 
            // sessionTimeout should be higher than the heartbeat interval.
            // The consumer cordinator should receive a heartbeat from every consumer
            // in a group. If the coordinator does not receive a heartbeat from
            // a consumer for a period of time, it will consider the consumer
            // to be failed and will rebalance the group.
            await heartbeat();
          }

          // Commit offsets for the batch
          await commitOffsetsIfNecessary();
        }
      });

      // Register graceful shutdown
      this.registerShutdown();

    } catch (error) {
      console.error(`[KafkaConsumerManager] Failed to start consumer for group "${this.groupId}":`, error);
      // Implement a backoff-retry strategy for reconnections if needed
      process.exit(1); // Exit if we can't connect initially
    }
  }

  /**
   * Registers signal handlers for graceful shutdown of the consumer and producer.
   */
  registerShutdown() {
    const errorTypes = ['unhandledRejection', 'uncaughtException'];
    const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

    errorTypes.forEach(type => {
      process.on(type, async e => {
        try {
          console.log(`[KafkaConsumerManager] Process event: ${type}`, e);
          await this.shutdown();
          process.exit(1);
        } catch (_) {
          process.exit(1);
        }
      });
    });

    signalTraps.forEach(type => {
      process.once(type, async () => {
        try {
          console.log(`[KafkaConsumerManager] Received ${type}, shutting down...`);
          await this.shutdown();
        } finally {
          // Allow default handlers to proceed
          process.kill(process.pid, type);
        }
      });
    });
  }

  /**
   * Disconnects the Kafka consumer and producer.
   */
  async shutdown() {
    try {
      console.log(`[KafkaConsumerManager] Disconnecting consumer and producer for group "${this.groupId}"...`);
      await this.consumer.disconnect();
      await this.producer.disconnect();
      console.log('[KafkaConsumerManager] Disconnected successfully.');
    } catch (error) {
      console.error('[KafkaConsumerManager] Error during shutdown:', error);
    }
  }

  // --- Internals ---
  async processWithRetry(fn, maxAttempts, backoffMs) {
    let attempt = 0;
    while (attempt < maxAttempts) {
      try {
        const res = await fn();
        return { ok: true, result: res, attempts: attempt + 1 };
      } catch (err) {
        attempt += 1;
        if (attempt >= maxAttempts) {
          return { ok: false, error: err, attempts: attempt };
        }
        // Exponential backoff
        const delay = backoffMs * Math.pow(2, attempt - 1);
        await new Promise(r => setTimeout(r, delay));
      }
    }
    return { ok: false, error: new Error('unknown'), attempts: attempt };
  }

  async sendToDLQ(topic, message, error, { attempts, partition }) {
    try {
      await this.producer.send({
        topic: `${topic}.dlq`,
        messages: [{
          key: message.key,
          value: message.value,
          headers: {
            'error': Buffer.from(String(error?.message || error)),
            'original-topic': Buffer.from(topic),
            'group-id': Buffer.from(this.groupId),
            'attempts': Buffer.from(String(attempts || 0)),
            'partition': Buffer.from(String(partition)),
            'offset': Buffer.from(String(message.offset)),
            'timestamp': Buffer.from(String(message.timestamp || Date.now()))
          }
        }]
      });
    } catch (produceErr) {
      console.error(`[Kafka: ${topic}]: Failed to send message to DLQ`, produceErr);
    }
  }
}

/**
 * Backward-compatible helper matching the previous syncHandler({ groupId, topic, syncFunction }) API.
 * Internally uses KafkaConsumerManager. Returns the manager instance for optional external control.
 */
export async function syncHandler({ groupId, topic, syncFunction }) {
  const manager = new KafkaConsumerManager(groupId);
  manager.registerHandler(topic, syncFunction);
  await manager.start();
  return manager;
}