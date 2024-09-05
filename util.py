import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def get_headers_and_types(table_name: str):
    cnx = mysql.connector.connect(user='root', password='Password01!', 
                              host='127.0.0.1', database='sakila')
    cursor = cnx.cursor()

    cursor.execute(f'DESCRIBE {table_name};')
    list_of_headers_and_types = []
    for row in cursor: 
        list_of_headers_and_types.append([row[0], row[1]])

    cursor.close()
    cnx.close()

    return list_of_headers_and_types

def produce_schema(list_of_headers_and_types: list):
    struct_fields = []

    for header_type_pair in list_of_headers_and_types:
        if 'int' in header_type_pair[1] or 'year' in header_type_pair[1]:
            struct_fields.append(StructField(header_type_pair[0], IntegerType(), True))
        elif 'varchar' in header_type_pair[1] or 'text' in header_type_pair[1]:
            struct_fields.append(StructField(header_type_pair[0], StringType(), True))
        elif 'set' in header_type_pair[1] or 'enum' in header_type_pair[1]:
            struct_fields.append(StructField(header_type_pair[0], StringType(), True))
        elif 'decimal' in header_type_pair[1]:
            struct_fields.append(StructField(header_type_pair[0], DecimalType(), True))
        elif 'timestamp' in header_type_pair[1]:
            struct_fields.append(StructField(header_type_pair[0], DateType(), True))

    return StructType(struct_fields)

 
def get_table_data(table_name: str):
    cnx = mysql.connector.connect(user='root', password='Password01!', 
                              host='127.0.0.1', database='sakila')
    cursor = cnx.cursor()

    cursor.execute(f'SELECT * FROM {table_name};')
    data = []
    for row in cursor:
        data.append(row)

    cursor.close()
    cnx.close()

    return data
