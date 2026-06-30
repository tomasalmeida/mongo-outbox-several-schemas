locals {
  # The shared outbox topic is named after the source collection, so the topic,
  # the SR subject (TopicNameStrategy -> "<topic>-value"), the connector's
  # TopicRegexRouter target, and the topic ACL all stay in sync.
  topic_name = var.mongodb_collection
}
