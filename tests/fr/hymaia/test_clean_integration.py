from pyspark.sql import Row
import pyspark.sql.functions as f
from tests.fr.hymaia.spark_test_case import spark 
from src.fr.hymaia.exo2.clean.spark_clean_job import read_files, filter_adults, join_city, add_department

def test_integration_pipeline():
    # Créer un DataFrame de test pour les clients
    clients_data = [
        Row(name="Alice", age=25, zip="75001"),
        Row(name="Bob", age=17, zip="75002"),  # Mineur, devrait être filtré
        Row(name="Charlie", age=30, zip="20190"),
        Row(name="David", age=35, zip="20190"),
        Row(name="Eve", age=45, zip="20200")
    ]
    clients_df = spark.createDataFrame(clients_data)

    # Créer un DataFrame de test pour les villes
    villes_data = [
        Row(zip="75001", city="Paris"),
        Row(zip="20190", city="Ajaccio"),
        Row(zip="20200", city="Bastia")
    ]
    villes_df = spark.createDataFrame(villes_data)

    # Étape 1: Filtrer les adultes
    adults_df = filter_adults(clients_df)
    assert adults_df.count() == 4  # Vérifier que Bob (le mineur) a été filtré

    # Étape 2: Joindre les villes
    joined_df = join_city(adults_df, villes_df)
    assert joined_df.filter(f.col("city") == "Ville inconnue").count() == 0  # Vérifier qu'il n'y a pas de "Ville inconnue"
    assert "city" in joined_df.columns  # Vérifier que la colonne "city" a été ajoutée

    # Étape 3: Ajouter le département
    result_df = add_department(joined_df)
    assert "departement" in result_df.columns  # Vérifier que la colonne "departement" a été ajoutée

    # Vérifier que les départements ont été correctement attribués
    results = result_df.collect()
    assert results[0]["departement"] == "75"  # Paris
    assert results[1]["departement"] == "2A"  # Corse 2A
    assert results[2]["departement"] == "2A"  # Corse 2A
    assert results[3]["departement"] == "2B"  # Corse 2B

    # Optionnel: Afficher les résultats pour vérification manuelle
    result_df.show(truncate=False)
