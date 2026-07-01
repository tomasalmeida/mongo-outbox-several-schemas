# ===========================================================================
# app-manager: used by Terraform to create the topic and register schemas.
# ===========================================================================
resource "confluent_service_account" "app_manager" {
  display_name = "${var.environment_name}-app-manager"
  description  = "Manages topics and schemas for the outbox environment (Terraform)."
}

resource "confluent_role_binding" "app_manager_env_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.this.resource_name
}

# Kafka API key for app-manager (used to create the topic via the REST endpoint).
resource "confluent_api_key" "app_manager_kafka" {
  display_name = "${var.environment_name}-app-manager-kafka-key"
  description  = "Kafka API key for managing the outbox topic."

  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  depends_on = [confluent_role_binding.app_manager_env_admin]
}

# Schema Registry API key for app-manager (used to register the JSON schemas).
resource "confluent_api_key" "app_manager_sr" {
  display_name = "${var.environment_name}-app-manager-sr-key"
  description  = "Schema Registry API key for registering outbox schemas."

  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.this.id
    api_version = data.confluent_schema_registry_cluster.this.api_version
    kind        = data.confluent_schema_registry_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }

  depends_on = [confluent_role_binding.app_manager_env_admin]
}

# ===========================================================================
# connector: identity the fully-managed MongoDB source connectors run as.
# ===========================================================================
resource "confluent_service_account" "connector" {
  display_name = "${var.environment_name}-connector"
  description  = "Identity used by the MongoDB Atlas source connectors."
}

# Allow the connector to produce to the shared outbox topic.
resource "confluent_role_binding" "connector_topic_write" {
  principal   = "User:${confluent_service_account.connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.this.rbac_crn}/kafka=${confluent_kafka_cluster.this.id}/topic=${local.topic_name}"
}

# Allow the connector to read the pre-registered schemas (use.latest.version=true
# means the serializer fetches the latest registered schema per subject).
resource "confluent_role_binding" "connector_sr_read" {
  principal   = "User:${confluent_service_account.connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.this.resource_name}/subject=${local.topic_name}-*"
}
