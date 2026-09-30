# Redshift Serverless (spec 09 §3.2). Publicly accessible but reachable only from
# allowed_cidr_blocks, with TLS enforced by the client (documented trade-off SR-02).

resource "aws_security_group" "redshift" {
  name        = "${local.name_prefix}-redshift"
  description = "Redshift Serverless: 5439 from the allowed CIDR blocks only"
  vpc_id      = data.aws_vpc.selected.id
}

resource "aws_vpc_security_group_ingress_rule" "redshift" {
  for_each = toset(var.allowed_cidr_blocks)

  security_group_id = aws_security_group.redshift.id
  description       = "Redshift from an allowed client"
  ip_protocol       = "tcp"
  from_port         = 5439
  to_port           = 5439
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_egress_rule" "redshift" {
  security_group_id = aws_security_group.redshift.id
  description       = "All outbound"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name = local.name_prefix
  db_name        = var.database_name

  # AWS generates the admin password and keeps it in Secrets Manager (never in state
  # outputs, code, or .env); the pipeline reads it via REDSHIFT_SECRET_ARN.
  admin_username        = var.redshift_admin_username
  manage_admin_password = true

  iam_roles            = aws_iam_role.redshift_s3_read[*].arn
  default_iam_role_arn = one(aws_iam_role.redshift_s3_read[*].arn)
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  workgroup_name      = local.name_prefix
  namespace_name      = aws_redshiftserverless_namespace.warehouse.namespace_name
  base_capacity       = var.redshift_base_capacity
  publicly_accessible = true
  subnet_ids          = local.subnet_ids
  security_group_ids  = [aws_security_group.redshift.id]
}

# Cost guardrail (CR-01): cap the daily compute of the workgroup.
resource "aws_redshiftserverless_usage_limit" "compute" {
  resource_arn  = aws_redshiftserverless_workgroup.warehouse.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_usage_limit_rpu_hours
  period        = "daily"
  breach_action = var.redshift_usage_limit_breach_action
}
