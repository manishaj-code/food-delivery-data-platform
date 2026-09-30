# Pipeline log group (spec 09 §3.4). The failure alarm and dashboard follow in Phase 14.

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days
}
