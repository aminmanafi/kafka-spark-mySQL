# kafka-spark-mySQL

This project reads streaming data from Apache Kafka using PySpark and writes the processed data into a MySQL database table. The project is structured in four different versions, each implementing different transformation strategies. All versions utilize a configuration file for flexible setup.

## Version Descriptions
### Version 1:
Reads data from Kafka, splits the incoming string using a configured delimiter to extract words, and writes the result to a MySQL table. The table schema is defined in the config file.

### Version 2:
Extends Version 1 by adding a transformation step: it computes the SHA1 hash of a specific column (as defined in the config file) and adds it as a new column. The result is written to MySQL. If the target table does not exist, it is automatically created.

### Version 3:
Similar to Version 1 in terms of transformations (word splitting based on delimiter), but instead of defining the schema in the config file, it reads the schema from an existing table in the MySQL database.

### Version 4:
Combines the features of Versions 2 and 3. It performs the SHA1 transformation as in Version 2 but retrieves the schema from an existing MySQL table, instead of using the config file.
