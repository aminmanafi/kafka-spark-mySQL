from pyspark.sql.functions import split, col, size
from schema_reader import get_table_schema
import config

# spliting the data by delimiter and making a dataframe
def process_data(spark, df):
    delimiter = config.data["delimiter"]

    # Get schema from the database (instead of config)
    schema = get_table_schema(spark)  # topic = table_name
    column_names = [field.name for field in schema]

    if not column_names:
        raise ValueError(f"No columns found in database table for topic")

    num_columns_expected = len(column_names)

    # Split the raw data using the delimiter
    df_split = df.withColumn("columns", split(col("raw_data"), delimiter))

    # Filter only rows with correct number of columns
    df_valid = df_split.filter(size(col("columns")) == num_columns_expected)

    # Add individual columns based on the database schema
    for i, col_name in enumerate(column_names):
        df_valid = df_valid.withColumn(col_name, col("columns")[i])

    return df_valid.select(column_names)
