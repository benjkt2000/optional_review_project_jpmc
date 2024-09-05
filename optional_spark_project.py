import mysql.connector
from pyspark.sql import SparkSession
from util import *

# SET UP SPARK SESSION
spark = SparkSession.builder \
    .master("local") \
    .appName("optional_project") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# CREATE DATAFRAMES
list_of_tables = ['actor', 'film', 'film_category', 'category']
list_of_dataframes = []

for table_name in list_of_tables:
    data = get_table_data(table_name)
    new_schema = produce_schema(get_headers_and_types(table_name))   
    new_df = spark.createDataFrame(data, new_schema)
    list_of_dataframes.append([table_name, new_df])

# CREATE VIEWS
    for df in list_of_dataframes:
        df[1].createOrReplaceTempView(df[0])

# QUERIES
print('QUESTION 1')
spark.sql('SELECT COUNT(DISTINCT last_name) AS NUMBER_OF_LAST_NAMES \
        FROM actor;').show()

print('QUESTION 2')
spark.sql('SELECT last_name \
        FROM actor \
        GROUP BY last_name \
        HAVING NOT COUNT(last_name) > 1;').show()

print('QUESTION 3')
spark.sql('SELECT last_name \
        FROM actor \
        GROUP BY last_name \
        HAVING COUNT(last_name) > 1;').show()

print('QUESTION 4')
spark.sql('SELECT AVG(length) AS AVERAGE_LENGTH_OF_FILMS \
          FROM film;').show()

print('QUESTION 5')
spark.sql('SELECT film_category.category_id, AVG(film.length) AS AVERAGE_LENGTH_OF_FILMS \
        FROM film \
        JOIN film_category \
        ON film.film_id = film_category.film_id \
        GROUP by film_category.category_id;').show()