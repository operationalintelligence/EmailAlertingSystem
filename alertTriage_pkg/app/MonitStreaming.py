import json
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class MonitStreaming:
    def consume(self,spark,schema,server_url):
        raw_data = spark.readStream.format("kafka").option("kafka.bootstrap.servers", server_url)\
            .option("subscribe", "cmsweb_logs").option("failOnDataLoss", False).load()\
            .select(from_json(col("value").cast("string"), schema).getField("metadata").alias("metadata")
                    .getField("host").alias("host"), col("timestamp").alias("timestamp"), from_json(col("value").cast("string"), schema)
                    .getField("data").alias("data").getField("system").alias("system"), from_json(col("value").cast("string"), schema)
                    .getField("data").alias("data").getField("dn").alias("user"), from_json(col("value").cast("string"), schema)
                    .getField("data").alias("data").getField("api").alias("api"))
        raw_data = raw_data.filter(~raw_data.system.rlike("^(%|/)"))
        return raw_data

    def windowGrouping(self,raw_data,watermark,time_interval,updated_time):
        return raw_data.withWatermark("timestamp", watermark).groupBy(window(
        'timestamp', time_interval, updated_time), "system", "api", "user").agg(count("api").alias("count_req"))

    def startMonit(self,streaming_df,hdfs_path,checkpoint_location):
        req_hdfs_flow = streaming_df.writeStream .outputMode("append").format("parquet")\
            .option("path", hdfs_path)\
            .option("checkpointLocation", checkpoint_location)\
            .outputMode("append").start()
        return req_hdfs_flow

    def stop(self,streaming_df):
        streaming_df.stop()
    
def main():
    monit=MonitStreaming()
    conf = (SparkConf()
            .setAppName("AlertingTriage")
            .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1"))
    sc = SparkContext(conf=conf).getOrCreate()
    spark = SparkSession.builder.appName(sc.appName).getOrCreate()
    schema = StructType().add("metadata", StructType().add("path", StringType()).add("_attachment_mimetype", StringType()).add("type_prefix", StringType()).add("host", StringType()).add("json", StringType()).add("producer", StringType()).add("topic", StringType()).add("_id", StringType()).add("type", StringType()).add("timestamp", LongType())).add(
        "data", StructType().add("code", StringType()).add("system", StringType()).add("uri_path", StringType()).add("method", StringType()).add("clientip", StringType()).add("client", StringType()).add("rec_date", StringType()).add("dn", StringType()).add("api", StringType()).add("rec_timestamp", StringType()).add("frontend", StringType()))
    raw_data = monit.consume(spark=spark,schema=schema,server_url="monit-kafka.cern.ch:9092")
    groupped_req_data = monit.windowGrouping(raw_data=raw_data,watermark="1 minute",time_interval="1 minute",updated_time="1 minute")
    req_hdfs_flow=monit.startMonit(streaming_df=groupped_req_data,hdfs_path="/cms/users/carizapo/ming/data_cmsweb_logs",checkpoint_location="/cms/users/carizapo/ming/req_checkpoint_cmsweb_logs")
    req_hdfs_flow.awaitTermination()
    
if __name__ == '__main__':
    main()