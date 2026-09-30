# `terraform test` (runs in CI): plans every create_* flag combination against a mocked
# AWS provider, so no credentials or account are needed (FR-122, spec 09 §3.3.1).

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_subnets" {
    defaults = {
      ids = ["subnet-a", "subnet-b", "subnet-c"]
    }
  }

  mock_resource "aws_redshiftserverless_workgroup" {
    defaults = {
      arn      = "arn:aws:redshift-serverless:ap-south-1:123456789012:workgroup/mock"
      endpoint = [{ address = "wg.example.invalid", port = 5439, vpc_endpoint = [] }]
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  allowed_cidr_blocks = ["203.0.113.7/32"]
}

run "sandbox_defaults" {
  command = plan

  assert {
    condition     = aws_s3_bucket.lake.bucket == "food-delivery-data-dev-123456789012"
    error_message = "Default bucket name must include the environment and account id."
  }

  assert {
    condition     = length(aws_iam_role.pipeline) == 0 && length(aws_iam_role.github_deploy) == 0
    error_message = "Sandbox defaults create no pipeline or GitHub deploy role."
  }

  assert {
    condition     = length(aws_iam_role.redshift_s3_read) == 1
    error_message = "The Redshift COPY role is created by default."
  }

  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.redshift) == 1
    error_message = "One ingress rule per allowed CIDR block."
  }

  assert {
    condition     = aws_redshiftserverless_workgroup.warehouse.base_capacity == 8
    error_message = "Base capacity defaults to the 8 RPU minimum."
  }

  assert {
    condition     = aws_redshiftserverless_usage_limit.compute.breach_action == "deactivate"
    error_message = "The usage limit deactivates the workgroup by default."
  }
}

run "monitoring" {
  command = plan

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.pipeline_failure.alarm_name == "food-delivery-dev-pipeline-failure"
      && aws_cloudwatch_metric_alarm.pipeline_failure.namespace == "FoodDelivery/Pipeline"
      && aws_cloudwatch_metric_alarm.pipeline_failure.metric_name == "PipelineFailure"
      && aws_cloudwatch_metric_alarm.pipeline_failure.statistic == "Sum"
      && aws_cloudwatch_metric_alarm.pipeline_failure.period == 86400
      && aws_cloudwatch_metric_alarm.pipeline_failure.threshold == 1
      && aws_cloudwatch_metric_alarm.pipeline_failure.treat_missing_data == "notBreaching"
    )
    error_message = "Failure alarm must match spec 12 §6."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.pipeline_failure.dimensions == tomap({ Environment = "dev" })
    error_message = "Alarm dimensions must match what src/common/metrics.py publishes."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.orders_quality[0].dimensions == tomap({ Environment = "dev", Dataset = "orders" })
    error_message = "The quality alarm watches the orders dataset."
  }

  assert {
    condition     = length(jsondecode(aws_cloudwatch_dashboard.pipeline[0].dashboard_body).widgets) == 6
    error_message = "Dashboard: ingested, rejected, score, processed, duration, success/failure."
  }
}

run "monitoring_optional_parts_off" {
  command = plan

  variables {
    create_quality_alarm = false
    create_dashboard     = false
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.orders_quality) == 0 && length(aws_cloudwatch_dashboard.pipeline) == 0
    error_message = "Quality alarm and dashboard are optional."
  }

  assert {
    condition     = output.dashboard_url == null
    error_message = "No dashboard, no URL."
  }
}

run "normal_account_all_roles" {
  command = plan

  variables {
    create_pipeline_role = true
    create_github_oidc   = true
    tf_state_bucket      = "food-delivery-tfstate"
  }

  assert {
    condition = (
      length(aws_iam_role.pipeline) == 1
      && length(aws_iam_openid_connect_provider.github) == 1
      && length(aws_iam_role.github_deploy) == 1
    )
    error_message = "All roles are created when the flags are on."
  }
}

run "no_iam_roles" {
  command = apply # mocked: the env_file output is only known after apply

  variables {
    create_redshift_s3_role = false
  }

  assert {
    condition     = length(aws_iam_role.redshift_s3_read) == 0
    error_message = "No COPY role: the pipeline uses REDSHIFT_COPY_AUTH=session."
  }

  assert {
    condition     = strcontains(output.env_file, "REDSHIFT_COPY_AUTH=session")
    error_message = "The .env output must switch COPY to session credentials."
  }
}

run "existing_vpc_without_default" {
  command = plan

  variables {
    vpc_id     = "vpc-0123456789abcdef0"
    subnet_ids = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
  }

  assert {
    condition     = aws_security_group.redshift.vpc_id == "vpc-0123456789abcdef0"
    error_message = "The security group belongs to the chosen VPC."
  }

  assert {
    condition     = toset(aws_redshiftserverless_workgroup.warehouse.subnet_ids) == toset(var.subnet_ids)
    error_message = "The workgroup uses exactly the chosen subnets."
  }
}

run "rejects_single_subnet" {
  command = plan

  variables {
    vpc_id     = "vpc-0123456789abcdef0"
    subnet_ids = ["subnet-0123456789abcdef0"]
  }

  expect_failures = [var.subnet_ids]
}

run "rejects_open_cidr" {
  command = plan

  variables {
    allowed_cidr_blocks = ["0.0.0.0/0"]
  }

  expect_failures = [var.allowed_cidr_blocks]
}
