# Data lake bucket (spec 09 §3.1, spec 11 §7): private, SSE-S3, TLS only, lifecycle expiry.

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = var.bucket_force_destroy
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ACLs disabled: the bucket owner owns every object.
resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

data "aws_iam_policy_document" "lake_tls_only" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.lake_tls_only.json

  # A bucket policy is rejected while Block Public Access is still being set up.
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

# Versioning stays disabled: the lake is reproducible from the source files.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  dynamic "rule" {
    for_each = {
      _tmp       = 1 # staging left behind by crashed runs
      validated  = 7
      quarantine = 90
      reports    = 365
    }

    content {
      id     = "expire-${trimprefix(rule.key, "_")}"
      status = "Enabled"

      filter {
        prefix = "${rule.key}/"
      }

      expiration {
        days = rule.value
      }
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
