spark = {
    "appName" : "project4"
}

kafka = {
    "bootstrap_servers" : "localhost:9092",
    "topic" : "topic1"
}

database = {
    "url" : "jdbc:mysql://localhost:3306/new_db",
    "user" : "root",
    "password" : "123",
    "driver" : "com.mysql.jdbc.Driver",
    "write_mode" : "append"
}

data = {
"delimiter" : ","
}

# topics =  {
#   "topic1": { "table": "topic1_table" },
#   "topic2": { "table": "topic2_table" }
# }

schema = {
    "topic1": ["id", "name", "dep_id"],
    "topic2": ["dep_id", "city"]
}

ext_columns = {
    "sha1" : "SHA1(name)",
}