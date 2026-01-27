from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, count, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# 1. Configuration & Session Setup
CHECKPOINT_PATH = "/tmp/spark-checkpoints/logs-processing"

spark = (
    SparkSession.builder
    .appName("LogsProcessor")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
    StructField("timestamp", LongType()), 
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# 3. Read Stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. Processing, Filtering & Aggregation
# filtering crash and severity
filtered_df = (
    raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) # needed, otherwise can't use "window" later on
    .filter(
        (lower(col("content")).contains("crash")) & 
        (col("severity").isin("High", "Critical"))
    )
)

# aggregating in 10 second-intervals, by user_id, crash_count per user_id
aggregated_df = (
    filtered_df
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("user_id")
    )
    .agg(count("*").alias("crash_count"))
    .filter(col("crash_count") > 2)
)

# 5. Writing
query = (
    aggregated_df.writeStream
    .outputMode("complete") 
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()