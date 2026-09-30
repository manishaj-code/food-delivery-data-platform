variable "environment" {
  description = "Deployment environment name, used in resource names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev"], var.environment)
    error_message = "environment must be \"dev\"."
  }
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-south-1"
}
