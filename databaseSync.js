import { KafkaJS } from "@confluentinc/kafka-javascript";
import { neo4jQuery } from "../../dbFuncs/neo4jFuncs";

// Kafka Config
const KAFKA_BROKERS = 'localhost:9092';
const SOURCE_TOPIC = "app_events_UserCreated";

const kafka = new KafkaJS.Kafka({ brokers: [KAFKA_BROKERS] });
const consumer = kafka.consumer({ groupId: 'neo4j-sync-group' });

await consumer.connect();
await consumer.subscribe({ topic: [SOURCE_TOPIC] });

await consumer.run({
  eachMessage: async ({ message }) => {
    console.log("[Kafka]: New message received: ", message.value.toString());
    const event = JSON.parse(message.value.toString());
    const { userId, username } = event;

    try{
      const result = await neo4jQuery(`MERGE (u:User {_id: ${userId}, username: ${username}, createdAt: datetime()}) RETURN u`, { userId, username }, "syncUserToNeo4j")
      console.log("[Kafka]: User synced to Neo4j: ", result);
    } catch(err) {
      console.log("[Kafka]: Error syncing user to Neo4j: ", err);
    }
  }
})

/**
 * HTTP/1.1 201 Created
Date: Wed, 30 Jul 2025 21:11:42 GMT
Location: http://localhost:8083/connectors/pg-outbox-connector
Content-Type: application/json
Content-Length: 622
Server: Jetty(9.4.54.v20240208)

{
  "name": "pg-outbox-connector",
  "config": {
    "connector.class":"io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname":"host.docker.internal",
    "database.port":"5432",
    "database.user":"postgres",
    "database.password":"Vivek2002&",
    "database.dbname":"Reelz",
    "topic.prefix":"pg-server",
    "table.include.list":"public.outbox",
    "plugin.name":"pgoutput",
    "transforms":"outbox",
    "transforms.outbox.type":"io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field":"event_type",
    "transforms.outbox.route.topic.replacement":"app_events_${routedByValue}",
    "name":"pg-outbox-connector"
  },
  "tasks":[],
  "type":"source"
}
 */