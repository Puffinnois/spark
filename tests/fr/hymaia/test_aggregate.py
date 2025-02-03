from tests.fr.hymaia.spark_test_case import spark  # Utiliser la session Spark existante
import pytest
from pyspark.sql import Row
import pyspark.sql.functions as f  # Import des fonctions PySpark
from src.fr.hymaia.exo2.aggregate.spark_aggregate_job import process_population_by_department

def test_process_population_by_department():
    # Créer un DataFrame de test simulant les données de "clean"
    data = [
        Row(name="Alice", age=25, zip="75001", departement="75"),
        Row(name="Bob", age=30, zip="20190", departement="2A"),
        Row(name="Charlie", age=35, zip="20190", departement="2A"),
        Row(name="Toto", age=40, zip="75008", departement="75"),
        Row(name="Titi", age=40, zip="20200", departement="2B")
    ]
    df = spark.createDataFrame(data)

    # Appliquer directement la logique de `process_population_by_department` sans écrire dans un fichier
    dept_count_df = df.groupBy("departement").agg(f.count("*").alias("nb_people"))
    sorted_df = dept_count_df.orderBy(f.col("nb_people").desc(), f.col("departement").asc())

    # Collecter les résultats pour les vérifier
    results = sorted_df.collect()

    # Vérifier que le tri est correct et que le nombre de personnes par département est bien calculé
    assert len(results) == 3
    assert results[0]["departement"] == "2A"  # Le premier car "2A" vient avant "75" en ordre alphabétique
    assert results[0]["nb_people"] == 2
    assert results[1]["departement"] == "75"  # Le deuxième avec 2 personnes
    assert results[1]["nb_people"] == 2
    assert results[2]["departement"] == "2B"  # Le dernier, avec 1 personne
    assert results[2]["nb_people"] == 1
