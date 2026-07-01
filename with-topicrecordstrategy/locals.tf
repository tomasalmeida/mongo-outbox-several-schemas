locals {
  # No TopicRegexRouter: the MongoDB source writes to its default topic, which is
  # "<database>.<collection>" (e.g. "tomas.outbox"). We derive topic_name the same
  # way so the created topic, the "<topic>-*" SR ACL, and the per-type subjects all
  # match what the connector actually produces to.
  #
  # With TopicRecordNameStrategy the SR subject is "<topic>-<recordName>", where the
  # recordName is the schema name our custom SMT stamps from the document's
  # `schemaName` field — so subjects become "<db>.<collection>-typeA/-typeB/-typeC".
  topic_name = "${var.mongodb_database}.${var.mongodb_collection}"
}
