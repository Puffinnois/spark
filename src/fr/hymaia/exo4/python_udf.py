import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

#Initialisation correcte de SparkSession
spark = SparkSession.builder \
    .appName("exo4_python_udf") \
    .master("local[*]") \
    .getOrCreate()

def main():
    start_time = time.time()

    #Charger les données CSV en explicitant les types
    df = spark.read.option("header", "true").csv("data/exo4/sell.csv")

    #Suppression propre des espaces autour des noms de colonnes
    df = df.select([col(c).alias(c.strip()) for c in df.columns])

    #Utiliser `when` au lieu d'une UDF pour de meilleures performances
    df_with_category_name = df.withColumn(
        "category_name",
        when(col("category").cast("int") < 6, "food").otherwise("furniture")
    )

    end_time = time.time()
    print(f"Temps d'exécution (sans UDF Python) : {end_time - start_time:.4f} secondes")

if __name__ == "__main__":
    main()
