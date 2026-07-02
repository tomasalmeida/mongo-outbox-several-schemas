# ---------------------------------------------------------------------------
# Reusable payload schemas, one registered subject per type.
# These are referenced by the umbrella schema below; they are NOT written to
# directly by the connector.
# ---------------------------------------------------------------------------
resource "confluent_schema" "type" {
  for_each = toset(var.outbox_types)

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.this.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.this.rest_endpoint

  subject_name = each.key
  format       = "JSON"
  schema       = file("${path.module}/schemas/${each.key}.json")

  credentials {
    key    = confluent_api_key.app_manager_sr.id
    secret = confluent_api_key.app_manager_sr.secret
  }
}

# ---------------------------------------------------------------------------
# Umbrella schema: a oneOf of references to the type schemas above.
#
# This is the schema the single connector serializes against. It is registered
# under the subject the connector writes to:
#   <topic>-value = outbox.events-value  (TopicNameStrategy)
#
# The $ref strings in outboxEvent.json ("typeA"/"typeB"/"typeC") are mapped to
# the registered subjects + versions via the schema_reference blocks.
# ---------------------------------------------------------------------------
resource "confluent_schema" "umbrella" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.this.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.this.rest_endpoint

  subject_name = "${local.topic_name}-value"
  format       = "JSON"
  schema       = file("${path.module}/schemas/outboxEvent.json")

  dynamic "schema_reference" {
    for_each = confluent_schema.type
    content {
      name         = schema_reference.key # must match the $ref in outboxEvent.json
      subject_name = schema_reference.value.subject_name
      version      = schema_reference.value.version
    }
  }

  credentials {
    key    = confluent_api_key.app_manager_sr.id
    secret = confluent_api_key.app_manager_sr.secret
  }
}
