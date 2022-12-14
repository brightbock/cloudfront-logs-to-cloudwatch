locals {
  # Abstracting out let's us create the lambda_logs log group even before the function exists
  lambda_function_name = "${var.project_name}_lambda"
}

resource "aws_cloudwatch_log_group" "cloudfront_logs" {
  name              = var.log_group_name
  retention_in_days = var.retention_in_days
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${local.lambda_function_name}"
  retention_in_days = var.lambda_log_retention_in_days
}

resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}_lambda_role"
  assume_role_policy = jsonencode(
    {
      Version = "2012-10-17",
      Statement = [
        {
          Action = "sts:AssumeRole",
          Principal = {
            Service = [
              "lambda.amazonaws.com"
            ]
          },
          Effect = "Allow",
          Sid    = "",
        }
      ]
    }
  )
}

resource "aws_iam_policy" "lambda_permissions" {
  name        = "${var.project_name}_lambda_permissions"
  path        = "/"
  description = "IAM policy for CFL to CWL lambda"

  policy = jsonencode(
    {
      Version = "2012-10-17",
      Statement = [
        {
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
          ],
          Resource = "arn:aws:logs:*:*:*",
          Effect   = "Allow"
        },
        {
          Action = [
            "s3:GetObject"
          ],
          Resource = var.s3_object_arn_list
          Effect   = "Allow"
        }
      ]
    }
  )
}

resource "aws_iam_role_policy_attachment" "lambda_permissions" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_permissions.arn
}

data "archive_file" "source_zip" {
  type        = "zip"
  source_file = var.lambda_src_file == "" ? "${path.module}/src/lambda.py" : var.lambda_src_file
  output_path = var.lambda_zip_file
}

resource "aws_lambda_function" "lambda_deploy" {
  description      = "Insert CloudFront logs in to CloudWatch Logs - Triggered by S3 Events"
  filename         = var.lambda_zip_file
  function_name    = local.lambda_function_name
  role             = aws_iam_role.lambda_execution_role.arn
  handler          = "${replace(basename(data.archive_file.source_zip.source_file), "/\\.py$/", "")}.lambda_handler"
  timeout          = var.lambda_timeout
  publish          = "true"
  memory_size      = var.lambda_memory_size
  architectures    = var.lambda_architectures
  source_code_hash = filebase64sha256(var.lambda_zip_file)
  runtime          = var.lambda_runtime
  environment {
    variables = {
      LOG_GROUP_NAME    = var.log_group_name
      EXCLUDE_SC_STATUS = var.exclude_sc_status
    }
  }
  lifecycle {
    create_before_destroy = true
  }
  depends_on = [
    aws_cloudwatch_log_group.cloudfront_logs,
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy_attachment.lambda_permissions
  ]
}

resource "aws_lambda_permission" "allow_s3_bucket" {
  count         = var.s3_bucket_name == "" ? 0 : 1
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_deploy.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.s3_bucket_name}"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  count  = var.s3_bucket_name == "" ? 0 : 1
  bucket = var.s3_bucket_name

  lambda_function {
    id                  = "tf-lambda-${local.lambda_function_name}"
    lambda_function_arn = aws_lambda_function.lambda_deploy.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.s3_notification_filter_prefix
    filter_suffix       = var.s3_notification_filter_suffix
  }

  depends_on = [
    aws_lambda_permission.allow_s3_bucket,
    aws_lambda_function.lambda_deploy
  ]
}

output "lambda_function_arn" {
  value = aws_lambda_function.lambda_deploy.arn
}
