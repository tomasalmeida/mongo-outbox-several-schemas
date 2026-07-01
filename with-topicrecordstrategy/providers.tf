# The Confluent provider authenticates with a Confluent Cloud (org-level) API key.
# Create one with: confluent api-key create --resource cloud
# You can also export CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET
# instead of passing these variables.
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}
