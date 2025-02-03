import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, sum
from pyspark.sql.window import Window

# Initialisation de la SparkSession
spark = SparkSession.builder \
    .appName("exo4_no_udf") \
    .master("local[*]") \
    .getOrCreate()

def main():
    start_time = time.time()  # Démarrer le chronomètre

    # Chargement des données depuis le fichier CSV (avec les données fournies)
    df = spark.read.option("header", "true").csv("data/exo4/sell.csv")

    # Suppression des espaces autour des noms de colonnes
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip())

    # Ajout de la colonne category_name sans utiliser d'UDF
    df_with_category_name = df.withColumn(
        "category_name",
        when(df["category"] < 6, "food").otherwise("furniture")
    )

    # Calcul de la somme des prix par catégorie et par jour
    window_day_category = Window.partitionBy("date", "category_name")
    df_with_total_per_day = df_with_category_name.withColumn(
        "total_price_per_category_per_day",
        sum("price").over(window_day_category)
    )

    # Calcul de la somme des prix pour les 30 derniers jours
    window_last_30_days = Window.partitionBy("category_name").orderBy("date").rowsBetween(-30, 0)
    df_with_total_per_day_last_30_days = df_with_total_per_day.withColumn(
        "total_price_per_category_per_day_last_30_days",
        sum("price").over(window_last_30_days)
    )

    # Déclencher une action pour forcer l'exécution et mesurer précisément le temps
    df_with_total_per_day_last_30_days.show()

    end_time = time.time()  # Fin du chronomètre

    # Affichage du temps d'exécution
    print(f"Temps d'exécution (sans UDF) : {end_time - start_time:.4f} secondes")

if __name__ == "__main__":
    main()
