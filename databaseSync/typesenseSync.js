import { syncHandler } from "../utilities/kafkaUtility.js";
import { syncTypeSense } from "../utilities/typesenseUtility.js";

const USER_CREATED_SOURCE_TOPIC = "app_events_UserCreated";

syncHandler({
  groupId: 'typesense-sync-group',
  topic: USER_CREATED_SOURCE_TOPIC,
  syncFunction: (event) => syncTypeSense(false, event.userId)
});