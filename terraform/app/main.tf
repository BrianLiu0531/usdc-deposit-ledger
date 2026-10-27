# The deposit pipeline: SQS FIFO (+ DLQ) -> Lambda (Go, arm64) -> DynamoDB, with CloudWatch alarms.
# Deployed by GitHub Actions with the role from ../bootstrap; state lives in that stack's S3 bucket.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
  # bucket, key and region are passed with -backend-config (see .github/workflows/deploy.yml)
  backend "s3" {
    use_lockfile = true
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

variable "name" {
  type    = string
  default = "usdc-ledger"
}

variable "watch_addresses" {
  description = "Accounts whose deposits and withdrawals are credited (lower-case 0x addresses)"
  type        = list(string)
  validation {
    condition     = alltrue([for a in var.watch_addresses : can(regex("^0x[0-9a-f]{40}$", a))])
    error_message = "Each address must be a lower-case 0x-prefixed 20-byte hex string."
  }
}

variable "lambda_zip" {
  description = "Path to the consumer build (bootstrap binary zipped)"
  type        = string
  default     = "../../dist/consumer.zip"
}

variable "alarm_email" {
  description = "Email for alarm notifications (empty = alarms without notifications)"
  type        = string
  default     = ""
}

locals {
  lambda_timeout = 30
}

# ---- DynamoDB: single-table ledger ----

resource "aws_dynamodb_table" "ledger" {
  name         = var.name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }
  attribute {
    name = "SK"
    type = "S"
  }

  # Idempotency markers expire after 30 days (longer than SQS can hold a message)
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
  server_side_encryption {
    enabled = true
  }
}

# ---- SQS FIFO queue and dead-letter queue ----

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-transfers-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600 # 14 days, the maximum, to give time to investigate
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "transfers" {
  name                        = "${var.name}-transfers.fifo"
  fifo_queue                  = true
  content_based_deduplication = false # the publisher sets a deduplication ID derived from the event ID
  # High-throughput FIFO: ordering and deduplication per message group (one group per transfer)
  deduplication_scope   = "messageGroup"
  fifo_throughput_limit = "perMessageGroupId"
  # AWS recommends at least 6x the function timeout so retries don't overlap an in-flight batch
  visibility_timeout_seconds = local.lambda_timeout * 6
  message_retention_seconds  = 345600 # 4 days
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.transfers.arn]
  })
}

# ---- Lambda consumer ----

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = "${var.name}-consumer"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "consumer" {
  statement {
    sid       = "Queue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [aws_sqs_queue.transfers.arn]
  }
  statement {
    sid = "Ledger"
    # TransactWriteItems is authorised per action inside the transaction
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:ConditionCheckItem", "dynamodb:GetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.ledger.arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.consumer.arn}:*"]
  }
}

resource "aws_iam_role_policy" "consumer" {
  name   = "consumer"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer.json
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${var.name}-consumer"
  retention_in_days = 14
}

resource "aws_lambda_function" "consumer" {
  function_name    = "${var.name}-consumer"
  role             = aws_iam_role.consumer.arn
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)
  timeout          = local.lambda_timeout
  memory_size      = 128

  environment {
    variables = {
      TABLE_NAME      = aws_dynamodb_table.ledger.name
      WATCH_ADDRESSES = join(",", var.watch_addresses)
    }
  }

  logging_config {
    log_format = "JSON"
    log_group  = aws_cloudwatch_log_group.consumer.name
  }

  depends_on = [aws_iam_role_policy.consumer]
}

resource "aws_lambda_event_source_mapping" "transfers" {
  event_source_arn = aws_sqs_queue.transfers.arn
  function_name    = aws_lambda_function.consumer.arn
  batch_size       = 10
  # Only the failed messages of a batch are retried (see internal/handler)
  function_response_types = ["ReportBatchItemFailures"]
  scaling_config {
    maximum_concurrency = 5
  }
}

# ---- Least-privilege policy for whatever runs the indexer ----

data "aws_iam_policy_document" "publisher" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.transfers.arn]
  }
}

resource "aws_iam_policy" "publisher" {
  name        = "${var.name}-publisher"
  description = "Lets the indexer's outbox relay send events to the transfers queue, and nothing else"
  policy      = data.aws_iam_policy_document.publisher.json
}

# ---- Alarms ----

resource "aws_sns_topic" "alarms" {
  name = "${var.name}-alarms"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name}-dlq-not-empty"
  alarm_description   = "A transfer event could not be credited after 5 attempts and needs investigation"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "backlog_age" {
  alarm_name          = "${var.name}-backlog-age"
  alarm_description   = "Deposits are waiting more than 5 minutes to be credited"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.transfers.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "consumer_errors" {
  alarm_name          = "${var.name}-consumer-errors"
  alarm_description   = "The consumer function is failing"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}

output "queue_url" {
  description = "Set as OUTBOX_SQS_QUEUE_URL on the indexer"
  value       = aws_sqs_queue.transfers.url
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "table_name" {
  value = aws_dynamodb_table.ledger.name
}

output "function_name" {
  value = aws_lambda_function.consumer.function_name
}

output "publisher_policy_arn" {
  description = "Attach to the IAM role or user that runs the indexer"
  value       = aws_iam_policy.publisher.arn
}
