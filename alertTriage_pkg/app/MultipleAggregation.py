
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,Window

class MultipleAggregation:
    def readHDFSStream(self, spark, schema, path):
        return spark.readStream.format("parquet").schema(schema).load(path)
    def readHDFSStatic(self, spark, path):
        return spark.read.format("parquet").load(path)

    def defineGrouping(self,static_df,observerPeriod,meanPeriod):
        obs_interval=window("window.start", observerPeriod)
        mean_interval=window("window.start", meanPeriod)

        wo_sys = Window.partitionBy('system',obs_interval)
        wo_user = Window.partitionBy('user',obs_interval)
        wo_api = Window.partitionBy('api',obs_interval)
        wo_req = Window.partitionBy('system','user','api',obs_interval)


        wm_sys = Window.partitionBy('system',mean_interval)
        wm_user = Window.partitionBy('user',mean_interval)
        wm_api = Window.partitionBy('api',mean_interval)
        wm_req = Window.partitionBy('system','user','api',mean_interval)
        groupped_df=static_df.filter("user!='null' and user!='-'").select('*', sum('count_req').over(wo_req).alias('req_load')).select('*', count('system').over(wo_sys).alias('system_load')).select('*', count('api').over(wo_api).alias('api_load')).select('*', count('user').over(wo_user).alias('user_load'))
        groupped_df=groupped_df.select('*', avg('req_load').over(wm_req).alias('avg_req')).select('*', ((col('req_load') - first('avg_req').over(wo_req))).alias('diff_req')).select('*', ((col('diff_req')/first('avg_req').over(wo_req))).alias('%diff_req')).select('*', avg('system_load').over(wm_sys).alias('avg_sys')).select('*', ((col('system_load') - first('avg_sys').over(wo_sys))).alias('diff_sys')).select('*', ((col('diff_sys')/first('avg_sys').over(wo_sys))).alias('%diff_sys')).select('*', avg('api_load').over(wm_api).alias('avg_api')).select('*', ((col('api_load') - first('avg_api').over(wo_api))).alias('diff_api')).select('*', ((col('diff_api')/first('avg_api').over(wo_api))).alias('%diff_api')).select('*', avg('user_load').over(wm_user).alias('avg_user')).select('*', ((col('user_load') - first('avg_user').over(wo_user))).alias('diff_user')).select('*', ((col('diff_user')/first('avg_user').over(wo_user))).alias('%diff_user'))
        return groupped_df
        
    def startAggregation(self,streamDF,staticDF,hdfsPath,checkpointPath):
        joined_raw_data=streamDF.join(staticDF, ["system","window","api","user","count_req"], "inner")
        full_difference_hdfs=joined_raw_data.writeStream.outputMode("append").format("parquet").option("path", hdfsPath).option("checkpointLocation", checkpointPath)  .option("failOnDataLoss",False) .outputMode("append")  .start()
        full_difference_hdfs.awaitTermination()

def main():
    multiAgg = MultipleAggregation()
    conf = (SparkConf()
        .setAppName("MultipleAggAlert")
        .set("spark.executor.memory", "10g")
        .set('spark.files.maxPartitionBytes', '10g')
        .set('spark.driver.maxResultSize', '10g'))
    sc = SparkContext(conf=conf).getOrCreate()
    spark = SparkSession.builder.appName(sc.appName).getOrCreate()
    schema = StructType().add("window",StructType().add("start",TimestampType()).add("end",TimestampType())).add("system", StringType()).add("api", StringType()).add("user", StringType()).add("count_req", LongType())
    raw_data = multiAgg.readHDFSStream(spark=spark,schema=schema,path="/cms/users/carizapo/ming/data_cmsweb_logs")
    static_data = multiAgg.readHDFSStatic(spark=spark,path="/cms/users/carizapo/ming/data_cmsweb_logs")

    columns_drop=['diff_user','diff_api','diff_sys','diff_req']
    groupped_df=multiAgg.defineGrouping(static_df=static_data,observerPeriod="1 hour",meanPeriod="1 day").drop(*columns_drop)
    multiAgg.startAggregation(staticDF=groupped_df,streamDF=raw_data,hdfsPath="/cms/users/carizapo/ming/fullDiff_cmsweb_logs",checkpointPath="/cms/users/carizapo/ming/checkpoint_prep_cmsweb_logs")

if __name__ == '__main__':
    main()