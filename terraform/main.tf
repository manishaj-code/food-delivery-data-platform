# Shared names and lookups (spec 09 §4 – §5). No networking is created: the workgroup uses
# the default VPC, or an existing VPC (vpc_id/subnet_ids) in accounts without one.

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_vpc" "selected" {
  id      = var.vpc_id
  default = var.vpc_id == null ? true : null
}

data "aws_subnets" "selected" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.selected.id]
  }
}

locals {
  name_prefix       = "food-delivery-${var.environment}"
  account_id        = data.aws_caller_identity.current.account_id
  partition         = data.aws_partition.current.partition
  bucket_name       = coalesce(var.bucket_name, "food-delivery-data-${var.environment}-${local.account_id}")
  log_group_name    = "/food-delivery/${var.environment}/pipeline"
  metrics_namespace = "FoodDelivery/Pipeline" # spec 12 §3, also in src/common/metrics.py
  subnet_ids        = coalesce(var.subnet_ids, data.aws_subnets.selected.ids)
}
