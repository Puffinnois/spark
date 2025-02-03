from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import get_clients_by_age, add_department_column, join_zip_code
from src.fr.hymaia.exo2.spark_aggregate_job import count_population_by_department
from pyspark.sql import Row


class TestClientMain(unittest.TestCase):
    def test_get_clients_by_age(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name='toto', age=20, zip='20100', city='Ajaccio'),
                Row(name='titi', age=18, zip='20100', city='Ajaccio'),
                Row(name='tata', age=17, zip='20200', city='Bastia')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='toto', age=20, zip='20100', city='Ajaccio'),
                Row(name='titi', age=18, zip='20100', city='Ajaccio')
            ]
        )

        # WHEN
        actual = get_clients_by_age(input, 18)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_join_zip_code(self):
        # GIVEN
        input_clients = spark.createDataFrame(
            [
                Row(name='toto', age=20, zip='20100', city='Ajaccio'),
                Row(name='titi', age=18, zip='20100', city='Ajaccio'),
                Row(name='tata', age=17, zip='20200', city='Bastia')
            ]
        )
        input_zip_code = spark.createDataFrame(
            [
                Row(zip='20100', city='Ajaccio'),
                Row(zip='20200', city='Bastia')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='toto', age=20, zip='20100', city='Ajaccio'),
                Row(name='titi', age=18, zip='20100', city='Ajaccio'),
                Row(name='tata', age=17, zip='20200', city='Bastia')
            ]
        )

        # WHEN
        actual = join_zip_code(input_clients, input_zip_code)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_add_department_column(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name='toto', age=20, zip='20100', city='Ajaccio'),
                Row(name='titi', age=18, zip='20100', city='Ajaccio'),
                Row(name='tata', age=17, zip='20200', city='Bastia')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='toto', age=20, zip='20100', city='Ajaccio', departement='2A'),
                Row(name='titi', age=18, zip='20100', city='Ajaccio', departement='2A'),
                Row(name='tata', age=17, zip='20200', city='Bastia', departement='2B')
            ]
        )

        # WHEN
        actual = add_department_column(input)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_count_population_by_department(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(departement='2A'),
                Row(departement='2A'),
                Row(departement='2B')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(departement='2A', nb_people=2),
                Row(departement='2B', nb_people=1)
            ]
        )

        # WHEN
        actual = count_population_by_department(input)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())