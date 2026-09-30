# Nothing secret is output: the Redshift admin password stays in Secrets Manager and only
# its ARN is shown.

output "aws_region" {
  value = var.aws_region
}

output "bucket_name" {
  description = "Data lake bucket (S3_BUCKET)."
  value       = aws_s3_bucket.lake.bucket
}

output "redshift_endpoint" {
  description = "Redshift Serverless workgroup endpoint (REDSHIFT_HOST)."
  value       = aws_redshiftserverless_workgroup.warehouse.endpoint[0].address
}

output "redshift_port" {
  value = aws_redshiftserverless_workgroup.warehouse.endpoint[0].port
}

output "redshift_database" {
  value = aws_redshiftserverless_namespace.warehouse.db_name
}

output "redshift_admin_secret_arn" {
  description = "Secrets Manager secret with the Redshift admin credentials (REDSHIFT_SECRET_ARN)."
  value       = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
}

output "redshift_s3_role_arn" {
  description = "Role Redshift COPY assumes (REDSHIFT_IAM_ROLE_ARN); null when create_redshift_s3_role = false."
  value       = one(aws_iam_role.redshift_s3_read[*].arn)
}

output "pipeline_role_arn" {
  description = "Role for the pipeline's AWS profile (role_arn); null when create_pipeline_role = false."
  value       = one(aws_iam_role.pipeline[*].arn)
}

output "github_deploy_role_arn" {
  description = "GitHub repository variable AWS_DEPLOY_ROLE_ARN; null when create_github_oidc = false."
  value       = one(aws_iam_role.github_deploy[*].arn)
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.pipeline.name
}

output "failure_alarm_name" {
  value = aws_cloudwatch_metric_alarm.pipeline_failure.alarm_name
}

output "dashboard_url" {
  description = "CloudWatch console link of the pipeline dashboard; null when create_dashboard = false."
  value = (
    var.create_dashboard
    ? "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards/dashboard/${aws_cloudwatch_dashboard.pipeline[0].dashboard_name}"
    : null
  )
}

output "env_file" {
  description = "AWS-mode lines for the untracked .env (terraform output -raw env_file)."
  value       = <<-EOT
    PIPELINE_ENV=${var.environment}
    STORAGE_MODE=s3
    WAREHOUSE_TYPE=redshift
    METRICS_ENABLED=true
    AWS_REGION=${var.aws_region}
    S3_BUCKET=${aws_s3_bucket.lake.bucket}
    REDSHIFT_HOST=${aws_redshiftserverless_workgroup.warehouse.endpoint[0].address}
    REDSHIFT_PORT=${aws_redshiftserverless_workgroup.warehouse.endpoint[0].port}
    REDSHIFT_DATABASE=${aws_redshiftserverless_namespace.warehouse.db_name}
    REDSHIFT_PASSWORD=
    REDSHIFT_SECRET_ARN=${aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn}
    REDSHIFT_IAM_ROLE_ARN=${var.create_redshift_s3_role ? aws_iam_role.redshift_s3_read[0].arn : ""}
    REDSHIFT_COPY_AUTH=${var.create_redshift_s3_role ? "iam_role" : "session"}
  EOT
}
