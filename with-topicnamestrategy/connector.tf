# ---------------------------------------------------------------------------
# Single fully-managed MongoDB Atlas Source connector.
#
# Flow:
#   1. pipeline: process inserts only (all schemaName values).
#   2. ExtractPayload: keep ONLY the `payload` object (schemaName stays in
#      Mongo, never serialized to Kafka).
#   3. RouteToOutbox: force the topic to local.topic_name so the subject is
#      deterministic.
#
# With TopicNameStrategy the subject becomes:
#   <topic>-value = <collection>-value
# which is the umbrella (oneOf of typeA/typeB/typeC references). The JSON Schema
# serializer validates each payload against the oneOf and picks the matching
# branch — so one connector + one subject carries all three reusable types.
# ---------------------------------------------------------------------------
resource "confluent_connector" "outbox_source" {
  environment {
    id = confluent_environment.this.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  config_sensitive = {
    "connection.password" = var.mongodb_password
  }

  config_nonsensitive = {
    # --- connector identity & class ---
    "connector.class" = "MongoDbAtlasSource"
    "name"            = "outbox-source"
    "tasks.max"       = "1"

    # --- run as the dedicated connector service account ---
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.connector.id

    # --- MongoDB Atlas connection ---
    "connection.host" = var.mongodb_connection_host
    "connection.user" = var.mongodb_user
    "database"        = var.mongodb_database
    "collection"      = var.mongodb_collection
    "startup.mode"    = var.connector_startup_mode

    # Publish only the document (the outbox row), not the change-stream envelope.
    "publish.full.document.only" = "true"

    # Process inserts only.
    "pipeline" = jsonencode([
      {
        "$match" = {
          "operationType" = "insert"
        }
      }
    ])

    # --- output format: JSON Schema (JSON_SR) so records carry a schema ---
    "output.data.format" = "JSON_SR"

    # --- SMT chain ---
    #   1. ExtractPayload: keep ONLY the payload object (schemaName stays in Mongo).
    #   2. CopyIdToKey: copy payload.id into the record key.
    #   3. ExtractKeyId: flatten the key to just the id value.
    #   4. RouteToOutbox: force the topic name so the subject is deterministic.
    "transforms" = "ExtractPayload,CopyIdToKey,ExtractKeyId,RouteToOutbox"

    "transforms.ExtractPayload.type"  = "org.apache.kafka.connect.transforms.ExtractField$Value"
    "transforms.ExtractPayload.field" = "payload"

    "transforms.CopyIdToKey.type"   = "org.apache.kafka.connect.transforms.ValueToKey"
    "transforms.CopyIdToKey.fields" = "id"

    "transforms.ExtractKeyId.type"  = "org.apache.kafka.connect.transforms.ExtractField$Key"
    "transforms.ExtractKeyId.field" = "id"

    "transforms.RouteToOutbox.type"        = "io.confluent.connect.cloud.transforms.TopicRegexRouter"
    "transforms.RouteToOutbox.regex"       = ".*"
    "transforms.RouteToOutbox.replacement" = local.topic_name

    # --- serialize against the pre-registered umbrella schema ---
    # TopicNameStrategy => subject is "<topic>-value" = "outbox.events-value".
    # (TopicRecordNameStrategy can't be used: the Mongo connector value is not a
    # named record schema, which that strategy requires.)
    "value.converter.value.subject.name.strategy" = "io.confluent.kafka.serializers.subject.TopicNameStrategy"
    "value.converter.auto.register.schemas"       = "false"
    "value.converter.use.latest.version"          = "true"
    "value.converter.latest.compatibility.strict" = "false"

    # Reject payloads that do not conform to the umbrella schema at serialize
    # time (instead of silently tagging them with the schema ID).
    "value.converter.json.fail.invalid.schema" = "true"

    # errors.tolerance=all => a record that fails validation/serialization is
    # skipped and logged rather than killing the task. NOTE: source connectors
    # have NO dead letter queue (DLQ is sink-only), so rejected records are
    # dropped and only visible in the connector logs. Set to "none" if you'd
    # rather the task hard-fail on the first bad record.
    "errors.tolerance"  = "all"
    "errors.log.enable" = "true"
  }

  # Schemas (types + umbrella) must exist (auto-register is off) and the topic +
  # ACLs must be in place before the connector starts producing.
  depends_on = [
    confluent_kafka_topic.outbox_events,
    confluent_schema.type,
    confluent_schema.umbrella,
    confluent_role_binding.connector_topic_write,
    confluent_role_binding.connector_sr_read,
  ]
}
