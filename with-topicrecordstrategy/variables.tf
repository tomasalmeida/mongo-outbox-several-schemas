# ---------------------------------------------------------------------------
# Confluent Cloud authentication (org-level Cloud API key)
# ---------------------------------------------------------------------------
variable "confluent_cloud_api_key" {
  type        = string
  description = "Confluent Cloud API key (org-level / 'cloud' resource)."
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud API secret (org-level / 'cloud' resource)."
  sensitive   = true
}

# ---------------------------------------------------------------------------
# Environment & cluster
# ---------------------------------------------------------------------------
variable "environment_name" {
  type        = string
  description = "Display name for the Confluent Cloud environment."
  default     = "outbox-pattern"
}

variable "stream_governance_package" {
  type        = string
  description = "Stream Governance (Schema Registry) package: ESSENTIALS or ADVANCED."
  default     = "ESSENTIALS"
}

variable "cluster_name" {
  type        = string
  description = "Display name for the Kafka cluster."
  default     = "outbox-cluster"
}

variable "cloud_provider" {
  type        = string
  description = "Cloud provider for the Kafka cluster (AWS, GCP, AZURE)."
  default     = "AWS"
}

variable "cloud_region" {
  type        = string
  description = "Cloud region for the Kafka cluster. Keep close to your MongoDB Atlas cluster."
  default     = "eu-west-1"
}

variable "cluster_availability" {
  type        = string
  description = "Cluster availability: SINGLE_ZONE or MULTI_ZONE."
  default     = "SINGLE_ZONE"
}

# ---------------------------------------------------------------------------
# Outbox topic & event types
# ---------------------------------------------------------------------------
# The outbox topic name is derived from the source collection (see locals.tf).

variable "topic_partitions" {
  type        = number
  description = "Partition count for the outbox topic."
  default     = 6
}

variable "outbox_types" {
  type        = list(string)
  description = <<-EOT
    The set of schemaName discriminator values found in the MongoDB outbox documents.
    One connector + one pre-registered JSON schema is created per type.
    Each name must have a matching schema file at ./schemas/<name>.json.
  EOT
  default     = ["typeA", "typeB", "typeC"]
}

# ---------------------------------------------------------------------------
# MongoDB Atlas source
# ---------------------------------------------------------------------------
variable "mongodb_connection_host" {
  type        = string
  description = "MongoDB Atlas SRV host WITHOUT the mongodb+srv:// prefix, e.g. 'cluster0.abcde.mongodb.net'."
}

variable "mongodb_user" {
  type        = string
  description = "MongoDB Atlas database user."
}

variable "mongodb_password" {
  type        = string
  description = "MongoDB Atlas database password."
  sensitive   = true
}

variable "mongodb_database" {
  type        = string
  description = "MongoDB database that holds the outbox collection."
  default     = "outbox_db"
}

variable "mongodb_collection" {
  type        = string
  description = "MongoDB collection holding outbox documents shaped like { payload: ..., schemaName: 'typeA' }."
  default     = "outbox"
}

variable "connector_startup_mode" {
  type        = string
  description = "MongoDB source startup mode: 'latest' (only new changes) or 'copy_existing' (snapshot first)."
  default     = "copy_existing"
}

# ---------------------------------------------------------------------------
# Custom SMT (uploaded out-of-band as a Connect artifact)
# ---------------------------------------------------------------------------
variable "smt_artifact_id" {
  type        = string
  description = <<-EOT
    ID of the uploaded custom-SMT Connect artifact (e.g. "cca-xxxxxx"), from
    `confluent connect artifact create/list`. Fully-managed connectors bind a
    custom SMT via `transforms.<name>.custom.smt.artifact.id` in ADDITION to the
    class name — without it, validation fails with "invalid transforms selected".
    The artifact must be READY before applying. Not managed by Terraform.
  EOT
}
