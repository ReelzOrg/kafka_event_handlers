import { syncHandler } from "../utilities/kafkaUtility.js";
import { neo4jQuery } from "../utilities/neo4jUtility.js";

const USER_CREATED_SOURCE_TOPIC = "app_events_UserCreated";

syncHandler({
  groupId: 'neo4j-sync-group',
  topic: USER_CREATED_SOURCE_TOPIC,
  syncFunction: (event) => neo4jQuery(`MERGE (u:User {_id: ${event.userId}, username: ${event.username}, createdAt: datetime()}) RETURN u`, { userId: event.userId, username: event.username }, "syncUserToNeo4j")
});

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