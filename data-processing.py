from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

import time

kafka_topic_name = "counts"
kafka_bootstrap_servers = 'localhost:9092'

cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "cctv"
cassandra_table_name = "cctv_counts"


def save_to_cassandra(current_df, epoc_id):
    print("Printing epoc id: ")
    print(epoc_id)

    print("Printing before Cassandra table save: " + str(epoc_id))
    current_df \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=cassandra_table_name, keyspace=cassandra_keyspace_name) \
        .save()
    print("Printing before Cassandra table save: " + str(epoc_id))


if __name__ == "__main__":
    print("Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .master("local[*]") \
        .config("spark.cassandra.connection.host", cassandra_host_name) \
        .config("spark.cassandra.connection.port", cassandra_port_no) \
        .getOrCreate()

    schema1 = StructType([
                          StructField('id', StringType(), True),
                          StructField('sensor_id', StringType(), True),
                          StructField('date_saved', StringType(), True),
                          StructField('time_saved', StringType(), True),
                          StructField('in_total', IntegerType(), True),
                          StructField('out_total', IntegerType(), True),
                          StructField('count_total', IntegerType(), True),
                          StructField('in_car', IntegerType(), True),
                          StructField('in_bus', IntegerType(), True),
                          StructField('in_med_truck', IntegerType(), True),
                          StructField('in_large_truck', IntegerType(), True),
                          StructField('in_jeepney', IntegerType(), True),
                          StructField('in_bike', IntegerType(), True),
                          StructField('in_tryke', IntegerType(), True),
                          StructField('in_others', IntegerType(), True),
                          StructField('out_car', IntegerType(), True),
                          StructField('out_bus', IntegerType(), True),
                          StructField('out_med_truck', IntegerType(), True),
                          StructField('out_large_truck', IntegerType(), True),
                          StructField('out_jeepney', IntegerType(), True),
                          StructField('out_bike', IntegerType(), True),
                          StructField('out_tryke', IntegerType(), True),
                          StructField('out_others', IntegerType(), True)])

    veh_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    veh_df1_5 = veh_df \
    .selectExpr("cast(value as string) value")

    veh_df2 = veh_df1_5 \
        .select(from_json(col("value"), schema1).alias("veh"))

    veh_df2.printSchema()

    veh_df3 = veh_df2.select("veh.*")
    veh_df3.printSchema()
    # .select(function.from_json($"value", Visit.Schema).as ("data"))

    print(veh_df.isStreaming)

    veh_df3 \
        .writeStream \
        .trigger(processingTime="15 seconds") \
        .outputMode("update") \
        .foreachBatch(save_to_cassandra) \
        .start().awaitTermination()