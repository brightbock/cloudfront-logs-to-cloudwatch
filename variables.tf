variable "project_name" {
  type    = string
  default = "cfl_to_cwl_demo"
}

variable "log_group_name" {
  type    = string
  default = "cloudfront_logs_from_s3"
}

variable "s3_object_arn_list" {
  type = set(string)
  # EXAMPLE = ["arn:aws:s3:::BUCKET/*.gz"]
}

variable "s3_bucket_name" {
  type    = string
  default = ""
}

variable "s3_notification_filter_prefix" {
  type    = string
  default = ""
}

variable "s3_notification_filter_suffix" {
  type    = string
  default = ".gz"
}

variable "retention_in_days" {
  type    = string
  default = "90"
  validation {
    condition     = contains(["1", "3", "5", "7", "14", "30", "60", "90", "120", "150", "180", "365", "400", "545", "731", "1827", "3653"], var.retention_in_days)
    error_message = "Variable must be one of: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653."
  }
}

variable "lambda_log_retention_in_days" {
  type    = string
  default = "7"
  validation {
    condition     = contains(["1", "3", "5", "7", "14", "30", "60", "90", "120", "150", "180", "365", "400", "545", "731", "1827", "3653"], var.lambda_log_retention_in_days)
    error_message = "Variable must be one of: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653."
  }
}

variable "exclude_sc_status" {
  type    = string
  default = ""
}

#### THE DEFAULTS SHOULD BE FINE BELOW HERE ####

variable "lambda_layers_python" {
  type    = list(string)
  default = []
}

variable "lambda_src_file" {
  type    = string
  default = ""
}

variable "lambda_zip_file" {
  type    = string
  default = ""
}

variable "lambda_memory_size" {
  type    = string
  default = "256"
}

variable "lambda_runtime" {
  type    = string
  default = "python3.11"
}

variable "lambda_architectures" {
  type    = set(string)
  default = ["arm64"]
}

variable "lambda_timeout" {
  type    = string
  default = "300"
}

