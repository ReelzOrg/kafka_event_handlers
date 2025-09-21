import { KafkaConsumerManager, syncHandler } from "../utilities/kafkaUtility.js";
import { neo4jQuery } from "../utilities/neo4jUtility.js";

const USER_CREATED_SOURCE_TOPIC = "app_events_UserCreated";
const POST_LIKED_SOURCE_TOPIC = "app_events_PostLiked";
const POST_COMMENTED_SOURCE_TOPIC = "app_events_PostCommented";

// syncHandler({
//   groupId: 'neo4j-sync-group',
//   topic: USER_CREATED_SOURCE_TOPIC,
//   syncFunction: (event) => neo4jQuery(`MERGE (u:User {_id: ${event.userId}, username: ${event.username}, createdAt: datetime()}) RETURN u`, { userId: event.userId, username: event.username }, "syncUserToNeo4j")
// });

const neo4jConsumerManager = new KafkaConsumerManager("neo4j-sync-group")
neo4jConsumerManager.registerHandler(USER_CREATED_SOURCE_TOPIC, (event) => {
  return neo4jQuery(
    `MERGE (u:User {_id: $userId})
    ON CREATE SET u.username = $username, u.createdAt = datetime()
    RETURN u`, 
    { userId: event.userId, username: event.username }, 
    "syncUserToNeo4j"
  );
});

// as the complexity of our recommendation algorithm increases, there should be a 
// weight assigned to the "like" to show that the like from this person is more
// important than the like from another person with less x (followers, connection strength, etc)
// syncHandler({
//   groupId: 'neo4j-sync-group',
//   topic: POST_LIKED_SOURCE_TOPIC,
//   syncFunction: (event) => neo4jQuery(`MATCH (p:Post {_id: ${event.postId}}) MATCH (u:User {_id: ${event.userId}}) CREATE (u)-[:LIKES]->(p) RETURN p`, { userId: event.userId, postId: event.postId }, "syncPostLikeToNeo4j")
// })

// const addLike = new KafkaConsumerManager("neo4j-likes-group")
neo4jConsumerManager.registerHandler(POST_LIKED_SOURCE_TOPIC, (event) => {
  return neo4jQuery(
    `MATCH (p:Post {_id: $postId})
    MATCH (u:User {_id: $userId})
    MERGE (u)-[r:LIKES]->(p)
    SET r.createdAt = datetime()
    RETURN p`, 
    { userId: event.userId, postId: event.postId }, 
    "syncPostLikeToNeo4j"
  );
});

// we are using MATCH in both like and comment because the post and user should
// already exist in the database before creating the relationship
// throw an error if they don't exist
// syncHandler({
//   groupId: 'neo4j-sync-group',
//   topic: POST_COMMENTED_SOURCE_TOPIC,
//   syncFunction: (event) => neo4jQuery(`MATCH (p:Post {_id: ${event.postId}}) MATCH (u:User {_id: ${event.userId}}) CREATE (u)-[:COMMENTED]->(p) RETURN p`, { userId: event.userId, postId: event.postId }, "syncPostCommentToNeo4j")
// })

// const addComment = new KafkaConsumerManager("neo4j-comments-group")
neo4jConsumerManager.registerHandler(POST_COMMENTED_SOURCE_TOPIC, (event) => {
  return neo4jQuery(
    `MATCH (p:Post {_id: $postId})
    MATCH (u:User {_id: $userId})
    MERGE (u)-[:COMMENTED]->(p)
    RETURN p`, 
    { userId: event.userId, postId: event.postId }, 
    "syncPostCommentToNeo4j"
  );
});

await neo4jConsumerManager.start();

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