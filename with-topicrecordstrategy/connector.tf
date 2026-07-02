# ---------------------------------------------------------------------------
# Single fully-managed MongoDB Atlas Source connector — TopicRecordNameStrategy.
#
# Flow:
#   1. pipeline: process inserts only.
#   2. NamePayload (CUSTOM SMT): read the document's `schemaName` discriminator,
#      promote the `payload` object to be the record value, and STAMP that
#      value's schema name with the discriminator (e.g. "typeA"). This is the
#      linchpin of this variant — see the note below.
#   3. CopyIdToKey / ExtractKeyId: key = payload.id.
#
# With TopicRecordNameStrategy the subject is derived from the record's schema
# NAME:
#   <topic>-<recordName> = <db>.<collection>-typeA / -typeB / -typeC
# so each type is serialized + validated against its own subject — no umbrella.
#
# There is no RouteToOutbox (a regex router would need re2j, which the custom-SMT
# runtime lacks): the connector writes to its default topic
# "<database>.<collection>" (see locals.tf).
#
# WHY A CUSTOM SMT (and not stock SetSchemaMetadata):
#   TopicRecordNameStrategy needs the value schema to have a name, but the Mongo
#   connector emits an UNNAMED value schema. Stock SetSchemaMetadata can set a
#   name, but only a STATIC one from config — it cannot read it per-record from
#   `schemaName`. And stock ExtractField (to keep only `payload`) DROPS the
#   schema name. So a single custom SMT both promotes `payload` and names its
#   schema from the `schemaName` field.
#
# PREREQUISITE: the custom SMT (./smt) must be uploaded to THIS environment as a
# Confluent Cloud "Connect artifact" BEFORE the connector is created, or the
# config is rejected with "invalid transforms selected: allowed transforms ...".
# Uploading adds the custom class to the allowed list. Upload with the CLI:
#   confluent connect artifact create outbox-smt \
#     --artifact-file smt/target/outbox-smt-1.0.0.jar \
#     --cloud aws --environment <env-id>
# Then WAIT until it is READY (it starts PROCESSING); the class is not allowed
# until then, so applying too early also 400s:
#   confluent connect artifact describe <artifact-id> --cloud aws --environment <env-id>
# Finally, pass the artifact ID as var.smt_artifact_id — the connector binds the
# SMT to the artifact via custom.smt.artifact.id below. There is NO Terraform
# resource for the upload, so it is a CLI step (see the README's two-phase apply).
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

    # Publish the whole outbox document (schemaName + payload). Unlike the
    # TopicNameStrategy variant we must KEEP schemaName here so the custom SMT
    # can read the discriminator; the SMT then drops it by promoting payload.
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
    #   1. NamePayload (custom): promote `payload`, name its schema from `schemaName`.
    #   2. CopyIdToKey: copy payload.id into the record key.
    #   3. ExtractKeyId: flatten the key to just the id value.
    "transforms" = "NamePayload,CopyIdToKey,ExtractKeyId"

    "transforms.NamePayload.type" = "io.confluent.examples.outbox.transforms.SetSchemaNameFromField"
    # Fully-managed connectors bind a custom SMT to its uploaded Connect artifact
    # via custom.smt.artifact.id. The control plane detects custom-SMT usage from
    # THIS key and provisions the runtime path; the class name alone is not enough
    # (validation would fail with "invalid transforms selected").
    "transforms.NamePayload.custom.smt.artifact.id" = var.smt_artifact_id
    "transforms.NamePayload.name.field"             = "schemaName" # value of this field becomes the schema name
    "transforms.NamePayload.value.field"            = "payload"    # promoted to be the new record value

    "transforms.CopyIdToKey.type"   = "org.apache.kafka.connect.transforms.ValueToKey"
    "transforms.CopyIdToKey.fields" = "id"

    "transforms.ExtractKeyId.type"  = "org.apache.kafka.connect.transforms.ExtractField$Key"
    "transforms.ExtractKeyId.field" = "id"

    # --- serialize against the pre-registered per-type schemas ---
    # TopicRecordNameStrategy => subject is "<topic>-<recordName>", and the
    # recordName is the schema name the custom SMT stamped ("typeA" -> subject
    # "<topic>-typeA"). No umbrella schema is involved.
    "value.converter.value.subject.name.strategy" = "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
    "value.converter.auto.register.schemas"       = "false"
    "value.converter.use.latest.version"          = "true"
    "value.converter.latest.compatibility.strict" = "false"
    "value.converter"                             = "io.confluent.connect.json.JsonSchemaConverter"

    # errors.tolerance=all => a record that fails serialization is skipped and
    # logged rather than killing the task. Source connectors have NO DLQ, so
    # rejected records are dropped (visible only in connector logs). Set to
    # "none" to hard-fail on the first bad record.
    "errors.tolerance"  = "none"
    "errors.log.enable" = "true"
    
  }

  # Per-type schemas must exist (auto-register is off) and the topic + ACLs must
  # be in place before the connector starts producing.
  depends_on = [
    confluent_kafka_topic.outbox_events,
    confluent_schema.type,
    confluent_role_binding.connector_topic_write,
    confluent_role_binding.connector_sr_read,
  ]
}
