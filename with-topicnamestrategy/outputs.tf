output "environment_id" {
  description = "Confluent Cloud environment ID."
  value       = confluent_environment.this.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID."
  value       = confluent_kafka_cluster.this.id
}

output "kafka_bootstrap_endpoint" {
  description = "Kafka bootstrap endpoint."
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  description = "Kafka REST endpoint."
  value       = confluent_kafka_cluster.this.rest_endpoint
}

output "schema_registry_rest_endpoint" {
  description = "Schema Registry REST endpoint."
  value       = data.confluent_schema_registry_cluster.this.rest_endpoint
}

output "outbox_topic" {
  description = "The shared outbox topic name."
  value       = confluent_kafka_topic.outbox_events.topic_name
}

output "type_schema_subjects" {
  description = "Referenced payload schema subjects (one per type)."
  value       = [for s in confluent_schema.type : s.subject_name]
}

output "umbrella_schema_subject" {
  description = "Umbrella (oneOf) subject the connector serializes against."
  value       = confluent_schema.umbrella.subject_name
}

output "connector_id" {
  description = "The MongoDB Atlas source connector ID."
  value       = confluent_connector.outbox_source.id
}

# Sensitive credentials — view with: terraform output -raw <name>
output "app_manager_kafka_api_key" {
  description = "app-manager Kafka API key."
  value       = confluent_api_key.app_manager_kafka.id
  sensitive   = true
}

output "app_manager_kafka_api_secret" {
  description = "app-manager Kafka API secret."
  value       = confluent_api_key.app_manager_kafka.secret
  sensitive   = true
}

output "app_manager_sr_api_key" {
  description = "app-manager Schema Registry API key."
  value       = confluent_api_key.app_manager_sr.id
  sensitive   = true
}

output "app_manager_sr_api_secret" {
  description = "app-manager Schema Registry API secret."
  value       = confluent_api_key.app_manager_sr.secret
  sensitive   = true
}
