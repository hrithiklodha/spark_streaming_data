from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.sql.shuffle.partitions',3). \
    config("spark.sql.catalogImplementation", "hive"). \
    enableHiveSupport(). \
    master('local[*]'). \
    getOrCreate()

orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark \
.readStream \
.format("json") \
.schema(orders_schema) \
.option("path", "data/inputdir") \
.load()

#Processing Logic
orders_df.createOrReplaceTempView("orders")
agg_orders = spark.sql("SELECT order_status,count(*) as total from orders group by order_status")

#Writing to the Sink
query = agg_orders \
.writeStream \
.format("console") \
.outputMode("update") \
.option("checkpointLocation", "checkpointdir1") \
.start()
query.awaitTermination()