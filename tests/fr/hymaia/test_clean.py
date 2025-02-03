from tests.fr.hymaia.spark_test_case import spark  # Utiliser la session Spark existante
import pytest
from pyspark.sql import Row
from src.fr.hymaia.exo2.clean.spark_clean_job import read_files, filter_adults, join_city, add_department, write_output

def test_read_files():
    # Appeler la fonction pour lire les fichiers
    clients_df, villes_df = read_files(spark)

    # Vérifier que les DataFrames ont bien été chargés
    assert clients_df is not None
    assert villes_df is not None

def test_filter_adults():
    # Créer un DataFrame de test avec des adultes et des mineurs
    data = [Row(name="Alice", age=25, zip="75001"),
            Row(name="Bob", age=17, zip="75002")]
    df = spark.createDataFrame(data)

    # Appliquer la fonction de filtrage
    adults_df = filter_adults(df)

    # Vérifier que seuls les adultes (>=18 ans) sont présents
    assert adults_df.count() == 1
    assert adults_df.collect()[0]["name"] == "Alice"

def test_join_city():
    # Créer des DataFrames de test pour clients et villes
    clients_data = [Row(name="Alice", age=25, zip="75001")]
    villes_data = [Row(zip="75001", city="Paris")]
    clients_df = spark.createDataFrame(clients_data)
    villes_df = spark.createDataFrame(villes_data)

    # Appliquer la jointure
    joined_df = join_city(clients_df, villes_df)

    # Vérifier que la ville a bien été jointe
    assert joined_df.count() == 1
    assert joined_df.collect()[0]["city"] == "Paris"

def test_add_department():
    # Créer un DataFrame de test avec des codes postaux
    data = [Row(zip="75001"), Row(zip="20190"), Row(zip="20200")]
    df = spark.createDataFrame(data)

    # Appliquer la fonction pour ajouter la colonne 'departement'
    df_with_dept = add_department(df)

    # Vérifier que la colonne 'departement' est correctement ajoutée
    results = df_with_dept.collect()
    assert results[0]["departement"] == "75"  # Paris
    assert results[1]["departement"] == "2A"  # Corse 2A
    assert results[2]["departement"] == "2B"  # Corse 2B

