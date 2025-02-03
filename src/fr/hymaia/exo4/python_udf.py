import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialisation de la SparkSession
spark = SparkSession.builder \
    .appName("exo4_python_udf") \
    .master("local[*]") \
    .getOrCreate()

# Définir une fonction Python pour attribuer les noms des catégories
def get_category_name(category):
    if int(category) < 6:
        return "food"
    return "furniture"

# Créer une UDF Spark à partir de la fonction Python
category_name_udf = udf(get_category_name, StringType())

def main():
    # Démarrer le chronomètre
    start_time = time.time()

    # Charger les données à partir du fichier CSV
    df = spark.read.option("header", "true").csv("data/exo4/sell.csv")

    # Suppression des espaces autour des noms de colonnes
    df = df.select([col.strip() for col in df.columns])

    # Application de la UDF Python pour ajouter une nouvelle colonne
    df_with_category_name = df.withColumn("category_name", category_name_udf(df["category"]))

    # Déclencher une action pour forcer le calcul
    df_with_category_name.show()

    # Mesurer le temps d'exécution
    end_time = time.time()
    print(f"Temps d'exécution (UDF Python): {end_time - start_time:.4f} secondes")

if __name__ == "__main__":
    main()
