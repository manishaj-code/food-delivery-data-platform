# IAM (spec 09 §3.3, spec 11 §2). Each role sits behind a create_* flag so the same code
# works in the sandbox account (spec 09 §3.3.1). No IAM users or access keys are created.

# --------------------------------------------------------------------------- pipeline role

data "aws_iam_policy_document" "pipeline_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = coalescelist(
        var.pipeline_role_trusted_principal_arns,
        ["arn:${local.partition}:iam::${local.account_id}:root"],
      )
    }
  }
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "LakeList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    sid       = "RedshiftAdminSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn]
  }

  statement {
    sid       = "RedshiftWorkgroup"
    actions   = ["redshift-serverless:GetCredentials", "redshift-serverless:GetWorkgroup"]
    resources = [aws_redshiftserverless_workgroup.warehouse.arn]
  }

  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource-level permissions

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [local.metrics_namespace]
    }
  }

  # Write for the pipeline; Describe/Get so optional Airflow remote logging can show the
  # task logs it wrote (spec 12 §1).
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
      "logs:GetLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.pipeline.arn}:*"]
  }
}

resource "aws_iam_role" "pipeline" {
  count = var.create_pipeline_role ? 1 : 0

  name               = "${local.name_prefix}-pipeline"
  description        = "Pipeline access to the lake, the Redshift secret, metrics, and logs"
  assume_role_policy = data.aws_iam_policy_document.pipeline_trust.json
}

resource "aws_iam_role_policy" "pipeline" {
  count = var.create_pipeline_role ? 1 : 0

  name   = "pipeline"
  role   = aws_iam_role.pipeline[0].id
  policy = data.aws_iam_policy_document.pipeline.json
}

# --------------------------------------------------------------------------- Redshift COPY role

data "aws_iam_policy_document" "redshift_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "redshift_s3_read" {
  statement {
    sid       = "ListProcessed"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["processed/*"]
    }
  }

  statement {
    sid       = "ReadProcessed"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/processed/*"]
  }
}

resource "aws_iam_role" "redshift_s3_read" {
  count = var.create_redshift_s3_role ? 1 : 0

  name               = "${local.name_prefix}-redshift-s3-read"
  description        = "Redshift COPY reads the processed/ zone of the lake"
  assume_role_policy = data.aws_iam_policy_document.redshift_trust.json
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  count = var.create_redshift_s3_role ? 1 : 0

  name   = "redshift-s3-read"
  role   = aws_iam_role.redshift_s3_read[0].id
  policy = data.aws_iam_policy_document.redshift_s3_read.json
}

# --------------------------------------------------------------------------- GitHub OIDC (deploy.yml)

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_trust" {
  count = var.create_github_oidc ? 1 : 0

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # apply runs in the dev environment; plan runs on main without an environment.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repository}:environment:${var.environment}",
        "repo:${var.github_repository}:ref:refs/heads/main",
      ]
    }
  }
}

# Terraform needs broad management rights, so scope is by name: project-prefixed IAM
# roles/policies, the lake bucket, the log group, and Redshift-managed secrets. Redshift
# Serverless and EC2 security-group actions do not support useful resource scoping at
# creation time, so they are limited by service instead (see docs/spec/11 §2).
data "aws_iam_policy_document" "github_deploy" {
  count = var.create_github_oidc ? 1 : 0

  statement {
    sid     = "LakeBucket"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*",
    ]
  }

  dynamic "statement" {
    for_each = var.tf_state_bucket == null ? [] : [var.tf_state_bucket]

    content {
      sid     = "TerraformState"
      actions = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
      resources = [
        "arn:${local.partition}:s3:::${statement.value}",
        "arn:${local.partition}:s3:::${statement.value}/*",
      ]
    }
  }

  statement {
    sid       = "RedshiftServerless"
    actions   = ["redshift-serverless:*"]
    resources = ["*"]
  }

  statement {
    sid = "Networking"
    actions = [
      "ec2:Describe*",
      "ec2:CreateSecurityGroup",
      "ec2:DeleteSecurityGroup",
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:CreateTags",
      "ec2:DeleteTags",
      "ec2:CreateVpcEndpoint",
      "ec2:DeleteVpcEndpoints",
      "ec2:ModifyVpcEndpoint",
    ]
    resources = ["*"]
  }

  statement {
    sid = "ProjectRoles"
    actions = [
      "iam:GetRole",
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:UpdateRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
      "iam:GetRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:PassRole",
    ]
    resources = ["arn:${local.partition}:iam::${local.account_id}:role/food-delivery-*"]
  }

  statement {
    sid = "OidcProvider"
    actions = [
      "iam:GetOpenIDConnectProvider",
      "iam:TagOpenIDConnectProvider",
      "iam:UpdateOpenIDConnectProviderThumbprint",
    ]
    resources = [aws_iam_openid_connect_provider.github[0].arn]
  }

  statement {
    sid       = "RedshiftServiceLinkedRole"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["arn:${local.partition}:iam::${local.account_id}:role/aws-service-role/redshift.amazonaws.com/*"]
  }

  statement {
    sid = "RedshiftManagedSecret"
    actions = [
      "secretsmanager:CreateSecret",
      "secretsmanager:DeleteSecret",
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetResourcePolicy",
      "secretsmanager:PutSecretValue",
      "secretsmanager:RotateSecret",
      "secretsmanager:TagResource",
      "secretsmanager:UpdateSecret",
    ]
    resources = ["arn:${local.partition}:secretsmanager:${var.aws_region}:${local.account_id}:secret:redshift!*"]
  }

  statement {
    sid = "PipelineLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:DeleteLogGroup",
      "logs:DescribeLogGroups",
      "logs:ListTagsForResource",
      "logs:PutRetentionPolicy",
      "logs:TagResource",
      "logs:UntagResource",
    ]
    resources = ["*"] # DescribeLogGroups is account-wide; the others only touch our group
  }

  statement {
    sid = "Monitoring"
    actions = [
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteAlarms",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:ListTagsForResource",
      "cloudwatch:TagResource",
      "cloudwatch:PutDashboard",
      "cloudwatch:GetDashboard",
      "cloudwatch:DeleteDashboards",
    ]
    resources = [
      "arn:${local.partition}:cloudwatch:${var.aws_region}:${local.account_id}:alarm:${local.name_prefix}-*",
      "arn:${local.partition}:cloudwatch::${local.account_id}:dashboard/${local.name_prefix}-*",
    ]
  }
}

resource "aws_iam_role" "github_deploy" {
  count = var.create_github_oidc ? 1 : 0

  name                 = "${local.name_prefix}-github-deploy"
  description          = "Terraform plan/apply from GitHub Actions (deploy.yml) via OIDC"
  assume_role_policy   = data.aws_iam_policy_document.github_trust[0].json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "github_deploy" {
  count = var.create_github_oidc ? 1 : 0

  name   = "terraform-deploy"
  role   = aws_iam_role.github_deploy[0].id
  policy = data.aws_iam_policy_document.github_deploy[0].json
}
