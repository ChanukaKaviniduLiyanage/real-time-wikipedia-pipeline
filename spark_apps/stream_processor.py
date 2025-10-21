# spark_apps/stream_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType(), True)
    ]), True),
    StructField("bot", BooleanType(), True)
])

def write_to_postgres(df, epoch_id):
    (df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/wikidb")
        .option("dbtable", "edit_counts_by_type")
        .option("user", "wikiuser")
        .option("password", "wikipassword")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save())

# Create a SparkSession
spark = (SparkSession.builder
         .appName("WikipediaStreamProcessor")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0")
         .getOrCreate())

# Read data from the Kafka topic
kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "wikipedia-edits")
            .option("startingOffsets", "latest")
            .load())

# Process the data
value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
exploded_df = value_df.selectExpr("data.meta.domain", "data.bot")

# group by both 'domain' and 'bot' status
edit_counts = (exploded_df
               .where("domain IS NOT NULL")
               .groupBy("domain", "bot")
               .count()
               .orderBy(col("count").desc()))

# Write the result to PostgreSQL using the foreachBatch function
query = (edit_counts.writeStream
         .outputMode("complete")
         .foreachBatch(write_to_postgres)
         .trigger(processingTime='45 seconds')
         .start())

query.awaitTermination()