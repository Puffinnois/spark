import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from fr.hymaia.exo2.transformations import clean_data, aggregate_data

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PARAM_1", "PARAM_2"])
    job.init(args['JOB_NAME'], args)

    param1 = args["PARAM_1"]
    param2 = args["PARAM_2"]

    print(f"Job parameters: PARAM_1={param1}, PARAM_2={param2}")

    # Reading input data from S3
    print("Reading input data...")
    df = spark.read.option("header", True).csv(param1)

    # Running transformations functions
    print("Running transformations...")
    cleaned_df = clean_data(df)       # Clean data function
    aggregated_df = aggregate_data(cleaned_df)  # Aggregate data function

    # Writing transformed data to S3
    print(f"Writing transformed data to {param2}...")
    aggregated_df.write.mode("overwrite").parquet(param2)

    job.commit()
    print("Job completed successfully.")