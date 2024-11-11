from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit, explode
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, ArrayType

spark = SparkSession\
    .builder\
    .appName("Kafka_Consumer")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")\
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("client_first_name", StringType(), True),
    StructField("client_last_name", StringType(), True),
    StructField("products", ArrayType(StructType([
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", FloatType(), True)
    ])), True),
    StructField("order_total", FloatType(), True),
    StructField("order_date_time", StringType(), True)
])

df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vendas")\
    .option("startingOffsets", "latest")\
    .load()
#to see all the messages from the beginning, change "latest" to "earliest" on "startingOffsets" line

df_orders = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("vendas")) \
    .select("vendas.*")

df_orders_exploded = df_orders.withColumn("product", explode("products")).select(
    "order_id",
    "client_first_name",
    "client_last_name",
    "order_total",
    "order_date_time",
    col("product.product").alias("product"),
    col("product.quantity").alias("quantity"),
    col("product.unit_price").alias("unit_price")
)

def process_batch(df, epoch_id):
     df.select(
        col("order_id"),
        col("client_first_name"),
        col("client_last_name"),
        col("product"),
        col("quantity"),
        col("unit_price"),
        col("order_total"),
        col("order_date_time")
    ).write \
     .format("org.apache.spark.sql.cassandra") \
     .options(table="orders", keyspace="vendas_keyspace") \
     .mode("append") \
     .save()


query = df_orders_exploded.writeStream.foreachBatch(process_batch).start()

query.awaitTermination()