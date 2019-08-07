import json
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from datetime import datetime
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class MonitStreaming:
    def consume(self,spark,schema,serverUrl):
        raw_data = spark.readStream.format("kafka").option("kafka.bootstrap.servers", serverUrl)\
            .option("subscribe", "cmsweb_logs").option("failOnDataLoss", False).load()\
            .select(from_json(col("value").cast("string"), schema).getField("metadata").alias("metadata")
                    .getField("host").alias("host"), col("timestamp").alias("timestamp"), from_json(col("value").cast("string"), schema)
                    .getField("data").alias("data").getField("system").alias("system"), from_json(col("value").cast("string"), schema)
                    .getField("data").alias("data").getField("dn").alias("user"), from_json(col("value").cast("string"), schema)
                    .getField("data").alias("data").getField("api").alias("api"))
        raw_data = raw_data.filter(~raw_data.system.rlike("^(%|/)"))
        return raw_data

    def windowGrouping(self,raw_data,watermark,timeInterval,updatedTime):
        return raw_data.withWatermark("timestamp", watermark).groupBy(window(
        'timestamp', timeInterval, updatedTime), "system", "api", "user").agg(count("api").alias("count_req"))

    def startMonit(self,streamingDF,hdfsPath,checkpointLocation):
        req_hdfs_flow = streamingDF.writeStream .outputMode("append").format("parquet")\
            .option("path", hdfsPath)\
            .option("checkpointLocation", checkpointLocation)\
            .outputMode("append").start()
        req_hdfs_flow.awaitTermination()

    def stop(self,streamingDF):
        streamingDF.stop()
    
    def main(self):
        conf = (SparkConf()
                .setAppName("AlertingTriage")
                .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1"))
        sc = SparkContext(conf=conf).getOrCreate()
        spark = SparkSession.builder.appName(sc.appName).getOrCreate()
        schema = StructType().add("metadata", StructType().add("path", StringType()).add("_attachment_mimetype", StringType()).add("type_prefix", StringType()).add("host", StringType()).add("json", StringType()).add("producer", StringType()).add("topic", StringType()).add("_id", StringType()).add("type", StringType()).add("timestamp", LongType())).add(
            "data", StructType().add("code", StringType()).add("system", StringType()).add("uri_path", StringType()).add("method", StringType()).add("clientip", StringType()).add("client", StringType()).add("rec_date", StringType()).add("dn", StringType()).add("api", StringType()).add("rec_timestamp", StringType()).add("frontend", StringType()))
        raw_data = self.consume(spark=spark,schema=schema,serverUrl="monit-kafka.cern.ch:9092")
        groupped_req_data = self.windowGrouping(raw_data=raw_data,watermark="1 minute",timeInterval="1 minute",updatedTime="1 minute")
        self.startMonit(streamingDF=groupped_req_data,hdfsPath="/cms/users/carizapo/ming/data_cmsweb_logs",checkpointLocation="/cms/users/carizapo/ming/req_checkpoint_cmsweb_logs")

if __name__ == '__main__':
    monit=MonitStreaming()
    monit.main()