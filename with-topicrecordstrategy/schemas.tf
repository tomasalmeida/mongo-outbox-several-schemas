# ---------------------------------------------------------------------------
# One registered subject per event type, under the TopicRecordNameStrategy
# naming convention:
#
#   subject = "<topic>-<recordName>" = "<db>.<collection>-typeA" / "-typeB" / "-typeC"
#
# The connector's custom SMT (SetSchemaNameFromField) stamps each record's value
# schema name from the document's `schemaName` field, so the JSON_SR serializer
# computes exactly these subjects and fetches the matching pre-registered schema
# (auto.register=false, use.latest.version=true).
#
# NOTE: there is NO umbrella / oneOf schema in this variant — that is the whole
# point of TopicRecordNameStrategy. Each type is serialized and validated against
# its own subject directly.
# ---------------------------------------------------------------------------
resource "confluent_schema" "type" {
  for_each = toset(var.outbox_types)

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.this.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.this.rest_endpoint

  # TopicRecordNameStrategy => subject is "<topic>-<recordName>".
  subject_name = "${local.topic_name}-${each.key}"
  format       = "JSON"
  schema       = file("${path.module}/schemas/${each.key}.json")

  credentials {
    key    = confluent_api_key.app_manager_sr.id
    secret = confluent_api_key.app_manager_sr.secret
  }
}
