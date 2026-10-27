# One-time setup, applied by a human with admin credentials:
#   - an S3 bucket for the app stack's Terraform state
#   - GitHub's OIDC identity provider and a deploy role that only this repo's main branch can assume,
#     so GitHub Actions deploys without any long-lived AWS keys
#   - a monthly budget alert

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "usdc-deposit-ledger", ManagedBy = "terraform" }
  }
}

variable "region" {
  type    = string
  default = "ap-southeast-2"
}

variable "github_repo" {
  description = "owner/name of the repository allowed to deploy"
  type        = string
  default     = "BrianLiu0531/usdc-deposit-ledger"
}

variable "github_subject_prefix" {
  description = <<-EOT
    The OIDC "sub" prefix GitHub issues for the repo. Newer repos use immutable IDs
    (repo:<owner>@<owner_id>/<repo>@<repo_id>), so a deleted and re-created repo with the same
    name cannot assume the role. Find it with: gh api repos/<owner>/<repo>/actions/oidc/customization/sub
  EOT
  type        = string
  default     = "repo:BrianLiu0531@49802872/usdc-deposit-ledger@1435454456"
}

variable "name" {
  description = "Prefix of every app resource; the deploy role can only touch resources with this prefix"
  type        = string
  default     = "usdc-ledger"
}

variable "budget_email" {
  description = "Where to send budget alerts (empty = no budget)"
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  type    = number
  default = 5
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account = data.aws_caller_identity.current.account_id
  arn     = "arn:${data.aws_partition.current.partition}"
}

# ---- Terraform state ----

resource "aws_s3_bucket" "state" {
  bucket = "${var.name}-tfstate-${local.account}"
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---- GitHub Actions OIDC ----

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    # Only workflows running on this repository's main branch
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "${var.github_subject_prefix}:ref:refs/heads/main",
        "repo:${var.github_repo}:ref:refs/heads/main", # repos still on the name-based format
      ]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.name}-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.github_trust.json
  max_session_duration = 3600
}

# Least privilege: the deploy role can manage only resources named "${var.name}-*"
data "aws_iam_policy_document" "deploy" {
  statement {
    sid       = "State"
    actions   = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
  }
  statement {
    sid     = "AppResources"
    actions = ["sqs:*", "dynamodb:*", "lambda:*", "logs:*", "sns:*", "cloudwatch:*"]
    resources = [
      "${local.arn}:sqs:${var.region}:${local.account}:${var.name}-*",
      "${local.arn}:dynamodb:${var.region}:${local.account}:table/${var.name}*",
      "${local.arn}:lambda:${var.region}:${local.account}:function:${var.name}-*",
      "${local.arn}:logs:${var.region}:${local.account}:log-group:/aws/lambda/${var.name}-*",
      "${local.arn}:logs:${var.region}:${local.account}:log-group:/aws/lambda/${var.name}-*:*",
      "${local.arn}:sns:${var.region}:${local.account}:${var.name}-*",
      "${local.arn}:cloudwatch:${var.region}:${local.account}:alarm:${var.name}-*",
    ]
  }
  statement {
    sid = "Describe"
    # These read-only calls do not support resource-level permissions
    actions = [
      "sqs:ListQueues", "dynamodb:ListTables", "dynamodb:DescribeLimits", "lambda:GetAccountSettings",
      "lambda:ListEventSourceMappings", "lambda:CreateEventSourceMapping", "lambda:GetEventSourceMapping",
      "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping", "lambda:TagResource",
      "logs:DescribeLogGroups", "cloudwatch:DescribeAlarms", "sns:ListTopics",
    ]
    resources = ["*"]
  }
  statement {
    sid = "AppRoles"
    actions = [
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:UpdateRole", "iam:TagRole", "iam:UntagRole",
      "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy", "iam:ListRolePolicies",
      "iam:AttachRolePolicy", "iam:DetachRolePolicy", "iam:ListAttachedRolePolicies", "iam:ListInstanceProfilesForRole", "iam:ListRoleTags",
      "iam:PassRole",
      "iam:CreatePolicy", "iam:DeletePolicy", "iam:GetPolicy", "iam:GetPolicyVersion", "iam:ListPolicyVersions",
      "iam:CreatePolicyVersion", "iam:DeletePolicyVersion", "iam:TagPolicy", "iam:ListPolicyTags",
    ]
    resources = [
      "${local.arn}:iam::${local.account}:role/${var.name}-*",
      "${local.arn}:iam::${local.account}:policy/${var.name}-*",
    ]
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "deploy-${var.name}"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}

# ---- Budget ----

resource "aws_budgets_budget" "monthly" {
  count        = var.budget_email == "" ? 0 : 1
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }
}

output "state_bucket" {
  value = aws_s3_bucket.state.bucket
}

output "deploy_role_arn" {
  value = aws_iam_role.deploy.arn
}
