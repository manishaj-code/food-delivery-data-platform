# Terraform and AWS provider versions (spec 10 §4). Resources are added in Phase 13.
# State is local by default; deploy.yml adds an S3 backend when TF_STATE_BUCKET is set (A-12).
terraform {
  required_version = ">= 1.16.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "food-delivery-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
