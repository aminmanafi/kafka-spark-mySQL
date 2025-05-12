from pyspark.sql.functions import split, col, size, expr
import config

# spliting the data by delimiter and making a dataframe
def process_data(df):

    # parameters
    delimiter = config.data["delimiter"]
    topic = config.kafka["topic"]
    column_names = config.schema[topic]
    # column_names = config.schema.get(topic)
    ext_columns = config.ext_columns
    

    if not column_names:
        raise ValueError(f"No schema defined in config for topic '{topic}'")

    num_columns_expected = len(column_names)

    # Split the raw data using the delimiter
    df_split = df.withColumn("columns", split(col("raw_data"), delimiter))

    # Filter only rows with correct number of columns
    df_valid = df_split.filter(size(col("columns")) == num_columns_expected)

    # Add individual columns based on the predefined schema
    for i, col_name in enumerate(column_names):
        df_valid = df_valid.withColumn(col_name, col("columns")[i])

    for key, value in ext_columns.items():
        df_valid = df_valid.withColumn(key, expr(value))
        column_names.append(key)
    
    return df_valid.select(column_names)
