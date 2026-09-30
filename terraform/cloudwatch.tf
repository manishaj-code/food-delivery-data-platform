# Monitoring (spec 09 §3.4, spec 12 §6): log group, failure alarm, optional quality alarm
# and dashboard. Metric dimensions must match src/common/metrics.py exactly: Environment
# (= PIPELINE_ENV, so set PIPELINE_ENV=<environment> in AWS mode) plus Dataset where the
# metric is per dataset. No SNS topic by default (see docs/monitoring.md to add email).

locals {
  metric_environment = { Environment = var.environment }
  source_datasets    = ["customers", "restaurants", "delivery_partners", "orders", "payments", "delivery"]
}

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_metric_alarm" "pipeline_failure" {
  alarm_name          = "${local.name_prefix}-pipeline-failure"
  alarm_description   = "A pipeline task failed after its retries (PipelineFailure >= 1 in a day)."
  namespace           = local.metrics_namespace
  metric_name         = "PipelineFailure"
  dimensions          = local.metric_environment
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching" # no failures reported = OK
  alarm_actions       = var.alarm_actions
  ok_actions          = var.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "orders_quality" {
  count = var.create_quality_alarm ? 1 : 0

  alarm_name          = "${local.name_prefix}-orders-quality"
  alarm_description   = "Orders data-quality score below ${var.quality_alarm_threshold}%."
  namespace           = local.metrics_namespace
  metric_name         = "DataQualityScore"
  dimensions          = merge(local.metric_environment, { Dataset = "orders" })
  statistic           = "Minimum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = var.quality_alarm_threshold
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions
  ok_actions          = var.alarm_actions
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  count = var.create_dashboard ? 1 : 0

  dashboard_name = "${local.name_prefix}-pipeline"
  dashboard_body = jsonencode({
    widgets = concat([
      for index, widget in [
        {
          title  = "Records ingested per dataset"
          metric = "RecordsIngested"
          stat   = "Sum"
        },
        {
          title  = "Records rejected per dataset"
          metric = "RecordsRejected"
          stat   = "Sum"
        },
        {
          title  = "Data-quality score (%)"
          metric = "DataQualityScore"
          stat   = "Minimum"
        },
        {
          title  = "Records processed per dataset"
          metric = "RecordsProcessed"
          stat   = "Sum"
        },
        ] : {
        type   = "metric"
        x      = (index % 2) * 12
        y      = floor(index / 2) * 6
        width  = 12
        height = 6
        properties = {
          title  = widget.title
          region = var.aws_region
          stat   = widget.stat
          period = 86400
          view   = "timeSeries"
          metrics = [
            for dataset in local.source_datasets :
            [local.metrics_namespace, widget.metric, "Environment", var.environment, "Dataset", dataset]
          ]
        }
      }
      ], [
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 12
        height = 6
        properties = {
          title  = "Pipeline duration (seconds)"
          region = var.aws_region
          stat   = "Maximum"
          period = 86400
          view   = "timeSeries"
          metrics = [
            [local.metrics_namespace, "PipelineDurationSeconds", "Environment", var.environment],
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 12
        width  = 12
        height = 6
        properties = {
          title  = "Runs: success vs failure"
          region = var.aws_region
          stat   = "Sum"
          period = 86400
          view   = "timeSeries"
          metrics = [
            [local.metrics_namespace, "PipelineSuccess", "Environment", var.environment],
            [local.metrics_namespace, "PipelineFailure", "Environment", var.environment],
          ]
        }
      },
    ])
  })
}
