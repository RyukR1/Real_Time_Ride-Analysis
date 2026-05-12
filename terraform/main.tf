terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  # Uncomment to use S3 backend for state
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "ride-analytics/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for Iceberg data lake
resource "aws_s3_bucket" "iceberg_bucket" {
  bucket = "ride-analytics-iceberg-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "ride-analytics-iceberg"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "iceberg_versioning" {
  bucket = aws_s3_bucket.iceberg_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_encryption" {
  bucket = aws_s3_bucket.iceberg_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# EC2 Instance for Spark jobs (optional, for on-demand processing)
resource "aws_instance" "spark_processor" {
  count           = var.enable_spark_instance ? 1 : 0
  ami             = data.aws_ami.ubuntu.id
  instance_type   = var.spark_instance_type
  security_groups = [aws_security_group.spark_sg.name]

  user_data = base64encode(templatefile("${path.module}/user_data.sh", {
    kafka_bootstrap = var.kafka_bootstrap_servers
  }))

  tags = {
    Name = "ride-analytics-spark-processor"
  }
}

# Security Group for Spark
resource "aws_security_group" "spark_sg" {
  name        = "ride-analytics-spark-sg"
  description = "Security group for Spark processor"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ride-analytics-spark-sg"
  }
}

# IAM Role for Spark to access S3
resource "aws_iam_role" "spark_role" {
  name = "ride-analytics-spark-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "spark_policy" {
  name = "ride-analytics-spark-policy"
  role = aws_iam_role.spark_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.iceberg_bucket.arn,
          "${aws_s3_bucket.iceberg_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# CloudWatch Log Group for monitoring
resource "aws_cloudwatch_log_group" "ride_analytics_logs" {
  name              = "/aws/ride-analytics/pipeline"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "ride-analytics-logs"
  }
}

# Data sources
data "aws_caller_identity" "current" {}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Outputs
output "iceberg_bucket_name" {
  description = "S3 bucket for Iceberg data lake"
  value       = aws_s3_bucket.iceberg_bucket.id
}

output "iceberg_bucket_arn" {
  description = "ARN of Iceberg bucket"
  value       = aws_s3_bucket.iceberg_bucket.arn
}

output "spark_instance_ip" {
  description = "Public IP of Spark processor instance"
  value       = try(aws_instance.spark_processor[0].public_ip, "N/A")
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group for pipeline"
  value       = aws_cloudwatch_log_group.ride_analytics_logs.name
}
