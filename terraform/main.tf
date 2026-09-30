# Shared names and lookups (spec 09 §4 – §5). Default VPC only: no custom networking.

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  name_prefix       = "food-delivery-${var.environment}"
  account_id        = data.aws_caller_identity.current.account_id
  partition         = data.aws_partition.current.partition
  bucket_name       = coalesce(var.bucket_name, "food-delivery-data-${var.environment}-${local.account_id}")
  log_group_name    = "/food-delivery/${var.environment}/pipeline"
  metrics_namespace = "FoodDelivery/Pipeline" # spec 12 §3, also in src/common/metrics.py
}
