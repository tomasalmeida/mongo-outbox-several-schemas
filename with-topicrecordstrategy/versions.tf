terraform {
  required_version = ">= 1.3.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.0.0, < 3.0.0"
    }
  }
}
