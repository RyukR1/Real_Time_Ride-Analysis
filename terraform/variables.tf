variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "enable_spark_instance" {
  description = "Whether to provision an EC2 instance for Spark"
  type        = bool
  default     = false
}

variable "spark_instance_type" {
  description = "EC2 instance type for Spark processor"
  type        = string
  default     = "m5.2xlarge"
}

variable "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers"
  type        = string
  default     = "localhost:9092"
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access Spark instance"
  type        = list(string)
  default     = ["0.0.0.0/0"] # Restrict this in production
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}
