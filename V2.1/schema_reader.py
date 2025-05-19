from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import config

def get_table_schema(spark) -> StructType:
   
    # JDBC URL
    jdbc_url = config.database["url"]
    table_name = f"{config.kafka["topic"]}_data"
    mysql_user = config.database["user"]
    mysql_password = config.database["password"]
    driver = config.database["driver"]

    # Connection properties
    connection_properties = {
        "user": mysql_user,
        "password": mysql_password,
        "driver": driver
    }

    # Read schema only using WHERE 1=0
    query = f"(SELECT * FROM {table_name} WHERE 1=0) AS temp"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

    return df.schema
