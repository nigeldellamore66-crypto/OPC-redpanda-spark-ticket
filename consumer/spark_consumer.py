from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("ClientTicketsToParquet")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("ticket_id", StringType())
    .add("client_id", IntegerType())
    .add("created_at", StringType())
    .add("demande", StringType())
    .add("type_demande", StringType())
    .add("priorite", StringType())
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "client-tickets")
    .option("startingOffsets", "earliest")
    .load()
)

tickets_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

enriched_df = tickets_df.withColumn(
    "support_team",
    when(col("type_demande") == "incident", "Team A")
    .when(col("type_demande") == "facturation", "Team B")
    .when(col("type_demande") == "technique", "Team C")
    .otherwise("Team D")
)

query = (
    enriched_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/data/parquet/client-tickets")
    .option("checkpointLocation", "/data/checkpoints/client-tickets")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()