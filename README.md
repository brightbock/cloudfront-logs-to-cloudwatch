![GitHub](https://img.shields.io/github/license/brightbock/cloudfront-logs-to-cloudwatch) ![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/brightbock/cloudfront-logs-to-cloudwatch) ![GitHub Workflow Status](https://img.shields.io/github/workflow/status/brightbock/cloudfront-logs-to-cloudwatch/Terraform)

# CloudFront logs to CloudWatch

This is a Terraform module / AWS [Lambda function](https://github.com/brightbock/cloudfront-logs-to-cloudwatch/blob/main/src/lambda.py) that reads [CloudFront standard access logs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html) as they are stored in to S3 by CloudFront, and inserts them in to CloudWatch Logs.

## How to use:

1. Add a module definition to your Terraform. See the example below.
2. Update the `BUCKET` placeholders to match the name of the S3 bucket storing CloudFront logs.


```
module "cloudfront_log_to_cloudwatch" {
  source = "git::https://github.com/brightbock/cloudfront-logs-to-cloudwatch.git?ref=v0.1.1"

  project_name       = "cloudfront_logs_demo"
  log_group_name     = "cloudfront_logs_demo"
  s3_object_arn_list = ["arn:aws:s3:::BUCKET/*.gz"]
  s3_bucket_name     = "BUCKET"
  lambda_memory_size = "128"
  # exclude_sc_status = "200,204,301,304"
}
```

### Notes:

- This Terraform module includes configuring a [`aws_s3_bucket_notification`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_notification) resource to configure a S3 bucket notification to trigger the lambda function. Your Terraform must only have one `aws_s3_bucket_notification` resource for each bucket. If you have existing S3 bucket notifications in Terraform, then set `s3_bucket_name = ""` here, and use the `lambda_function_arn` output of this module to add a `lambda_function` stanza to your existing `aws_s3_bucket_notification` resource.
- `exclude_sc_status` is a comma separated list of HTTP status code _prefixes_. Logs with HTTP status matching this list will not be sent to CloudWatch Logs. For example `exclude_sc_status = "2,30,404"` would exclude logs with 2xx, 30x, and 404 status codes.
- Understand Cloudwatch Logs pricing before deploying this.
- Various other solutions exist:
  - [Using Amazon Athena to query Cloudfront logs](https://docs.aws.amazon.com/athena/latest/ug/cloudfront-logs.html)
  - [CloudFront real-time logs in OpenSearch / Kibana](https://aws.amazon.com/blogs/networking-and-content-delivery/cloudfront-realtime-logs/)

