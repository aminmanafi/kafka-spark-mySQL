import config

def write_to_mysql(df):

    # parameters
    url = config.database["url"]
    topic = config.kafka["topic"]
    db_user = config.database["user"]
    db_password = config.database["password"]
    db_driver = config.database["driver"]
    write_mode = config.database["write_mode"]

    def batch_writing(batch_df, epoch_id):
        # spark = batch_df.sparkSession

        # # Query MySQL to check if the table exists
        # table_check_df = spark.read \
        #     .format("jdbc") \
        #     .option("url", url) \
        #     .option("query", f"""
        #         SELECT COUNT(*) as count 
        #         FROM information_schema.tables 
        #         WHERE table_schema = '{url.split('/')[-1]}' 
        #           AND table_name = '{f"{topic}_data"}'
        #     """) \
        #     .option("user", db_user) \
        #     .option("password", db_password) \
        #     .option("driver", db_driver) \
        #     .load()

        # table_exists = table_check_df.collect()[0]["count"] > 0

        # if not table_exists:
        #     # Create the table using the schema from the batch_df
        #     mode = 'overwrite'
        # else:
        #     # Append data normally
        #     mode = 'append'
            
        batch_df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{topic}_data") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", db_driver) \
            .mode(write_mode) \
            .save()

    query = df.writeStream \
        .outputMode(write_mode) \
        .foreachBatch(batch_writing) \
        .start()

    query.awaitTermination()
