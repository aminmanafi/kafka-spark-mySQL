from kafka_reader import read_from_kafka
from data_parser import process_data
from db_writer import write_to_mysql
import config

def main():

    spark, kafka_df = read_from_kafka()

    parsed_df = process_data(kafka_df)

    write_to_mysql(parsed_df)

if __name__ == "__main__":
    main()
