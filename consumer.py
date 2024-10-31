from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit, explode
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, ArrayType

spark = SparkSession\
    .builder\
    .appName("Kafka_Consumer")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")\
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

df_result = df_orders_exploded.withColumn(
    "purchase_summary",
    concat(
        lit("Total da compra de "),
        col("client_first_name"),
        lit(" "),
        col("client_last_name"),
        lit(": R$ "),
        col("order_total"),
        lit(" reais")
    )
).dropDuplicates(["purchase_summary"])


def process_batch(df, epoch_id):
    for row in df.select("purchase_summary").collect():
        print(row["purchase_summary"])


query = df_result.writeStream.foreachBatch(process_batch).start()

query.awaitTermination()