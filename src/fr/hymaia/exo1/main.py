import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    input_file = "src/resources/exo1/data.csv"
    df = spark.read.csv(input_file, header=False, inferSchema=True) \
        .toDF("text")

    result_df = wordcount(df, "text")

    output_dir = "data/exo1/output"
    result_df.write.partitionBy("count").parquet(output_dir)

    result = result_df.select('word', 'count').rdd.map(tuple).collect()

    print(result)

    spark.stop()

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
