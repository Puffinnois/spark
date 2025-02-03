import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def count_population_by_department(df):
    return df.groupBy("departement") \
        .agg(f.count("*").alias("nb_people")) \
        .orderBy(f.desc("nb_people"), f.asc("departement"))

def main():
    spark = SparkSession.builder \
        .appName("aggregate_population_by_department") \
        .master("local[*]") \
        .getOrCreate()

    input_file = "data/exo2/clean"

    df_clean = spark.read.parquet(input_file)

    df_population_by_department = count_population_by_department(df_clean)

    output_directory = "data/exo2/aggregate"

    df_population_by_department.coalesce(1).write.csv(output_directory, header=True)

    spark.stop()