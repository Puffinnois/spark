resource "aws_glue_job" "spark_handson_job" {
  name     = "spark-handson-job" # Nom du job
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.spark_handson_bucket.bucket}/jobs/exo2_glue_job.py"
    python_version  = "3"
  }

  glue_version      = "3.0"
  number_of_workers = 2
  worker_type       = "Standard"

  default_arguments = {
    "--job-language"                    = "python"
    "--additional-python-modules"       = "s3://${aws_s3_bucket.spark_handson_bucket.bucket}/wheel/spark_handson-0.1.0-py3-none-any.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--JOB_NAME"                        = "spark_handson_job"
    "--PARAM_1"                         = "value1"
    "--PARAM_2"                         = "value2"
  }

  tags = {
    Name        = "spark-handson-job"
    Environment = "dev"
  }
}
