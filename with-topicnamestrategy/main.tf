# ---------------------------------------------------------------------------
# Environment (with Stream Governance / Schema Registry enabled)
# ---------------------------------------------------------------------------
resource "confluent_environment" "this" {
  display_name = var.environment_name

  stream_governance {
    package = var.stream_governance_package
  }
}

# ---------------------------------------------------------------------------
# Basic Kafka cluster
# ---------------------------------------------------------------------------
resource "confluent_kafka_cluster" "this" {
  display_name = var.cluster_name
  availability = var.cluster_availability
  cloud        = var.cloud_provider
  region       = var.cloud_region

  standard {}

  environment {
    id = confluent_environment.this.id
  }
}

# ---------------------------------------------------------------------------
# Schema Registry cluster (auto-provisioned by Stream Governance on the env)
# ---------------------------------------------------------------------------
data "confluent_schema_registry_cluster" "this" {
  environment {
    id = confluent_environment.this.id
  }

  # Ensure the environment (and thus SR) is provisioned before reading it.
  depends_on = [confluent_kafka_cluster.this]
}

# ---------------------------------------------------------------------------
# The single shared outbox topic.
# All three event types land here; TopicRecordNameStrategy keeps their
# schemas separate via per-record subjects (<topic>-typeA, -typeB, -typeC).
# ---------------------------------------------------------------------------
resource "confluent_kafka_topic" "outbox_events" {
  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }
  topic_name       = local.topic_name
  partitions_count = var.topic_partitions
  rest_endpoint    = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.app_manager_kafka.id
    secret = confluent_api_key.app_manager_kafka.secret
  }
}
