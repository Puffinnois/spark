resource "aws_s3_bucket" "spark_handson_bucket" {
  bucket = "spark-handson-bucket-unique-name"

  tags = {
    Name        = "spark-handson-bucket"
    Environment = "dev"
  }
}
