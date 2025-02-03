import time
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

# Initialisation de la SparkSession avec le fichier JAR pour l'UDF Scala
spark = SparkSession.builder \
    .appName("exo4") \
    .master("local[*]") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()


def addCategoryName(col):
    # Récupération du SparkContext
    sc = spark.sparkContext
    # Chargement de la fonction Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # Retourne un objet colonne avec l'application de l'UDF Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    # Chargement des données depuis le fichier CSV
    df = spark.read.option("header", "true").csv("data/exo4/sell.csv")

    # Suppression des espaces entourant les noms des colonnes
    df = df.select([f.col(c.strip()).alias(c.strip()) for c in df.columns])
    
    # Mesure du temps d'exécution pour l'application de l'UDF Scala
    start_time = time.time()

    # Application de l'UDF Scala pour ajouter la colonne category_name
    df_with_category_name = df.withColumn("category_name", addCategoryName(df["category"]))

    # Déclencher une action pour mesurer précisément le temps (comme une écriture ou un affichage)
    df_with_category_name.show()

    end_time = time.time()

    # Affichage du temps d'exécution
    print(f"Temps d'exécution (UDF Scala): {end_time - start_time:.4f} secondes")


if __name__ == "__main__":
    main()
