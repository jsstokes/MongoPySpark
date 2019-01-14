
# Here's the command used to  start the PySpark session
# 
# pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/PresidentialCampaign.donations?readPreference=primaryPreferred" \
#         --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/PresidentialCampaign.myCollection" \
#         --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1



# Create a dataframe and write to MongoDB
people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
   ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()


# Read from the default datasource
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

df.printSchema()

# Use the Presidential Campaign donations with no filtering
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
"mongodb://127.0.0.1/PresidentialCampaign.donations").load()


df.printSchema()

# Add the option to use an aggregation pipeline
pipeline = "{'$match': {'contributor_state': 'CT'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
"mongodb://127.0.0.1/PresidentialCampaign.donations").option("pipeline", pipeline).load()
df.show()


# Use a filter on the dataframe
df.filter(df['amount'] > 50).show()

df.filter(df['contributor_state'] == 'CT').show()

# can we use SQL statements
# registers a temporary table called temp, then uses SQL to query for records
df.createOrReplaceTempView("temp")
CT_state = spark.sql("SELECT amount, contributor_city FROM temp WHERE contributor_state = 'CT'")
CT_state.show()

df.createOrReplaceTempView("temp1")
avg_state = spark.sql("SELECT contributor_state, avg(amount) FROM temp group by contributor_state")
avg_state.show()

