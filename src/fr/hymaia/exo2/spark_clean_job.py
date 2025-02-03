import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def get_clients_by_age(df, age):
    return df.filter(df['age'] >= age)

def add_department_column(df):
    return df.withColumn("departement",
                         f.when(f.col("zip").substr(1, 2) == "20",
                         f.when(f.col("zip") <= "20190", "2A")
                         .otherwise("2B"))
                         .otherwise(f.col("zip").substr(1, 2)))

def join_zip_code(df_clients, df_zip_code):
    return df_clients.join(df_zip_code, on="zip", how='inner') \
        .select(df_clients['name'], df_clients['age'], df_clients['zip'], df_zip_code['city'])

def main():
    spark = SparkSession.builder \
        .appName("get_clients_by_age") \
        .master("local[*]") \
        .getOrCreate()

    input_zip_code_file = "src/resources/exo2/city_zipcode.csv"
    input_clients_file = "src/resources/exo2/clients_bdd.csv"
    output_directory = "data/exo2/clean"

    df_zip_code = spark.read.csv(input_zip_code_file, header=True, inferSchema=True)
    df_clients = spark.read.csv(input_clients_file, header=True, inferSchema=True)

    df_clients_filtered = get_clients_by_age(df_clients, 18)

    df_clients_filtered = join_zip_code(df_clients_filtered, df_zip_code)

    df_clients_with_department = add_department_column(df_clients_filtered)

    df_clients_with_department.write.mode("overwrite").parquet(output_directory)

    spark.stop()