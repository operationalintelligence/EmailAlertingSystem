from pyspark.sql.functions import *
from pyspark.sql.types import *
from Custom_transformers import DateConverter,HourExtractor,MinExtractor,DayExtractor,MonthExtractor,YearExtractor,\
                               WeekDayExtractor,WeekendExtractor,MonthBeginExtractor,MonthEndExtractor
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler, StandardScaler
from pyspark.ml.clustering import KMeans
from numpy import array
from math import sqrt
from scipy.spatial.distance import euclidean
from notifier import Notifier 
import json


class StreamingAlert:
    def readHDFSStream(self, spark, schema, path):
        return spark.readStream.format("parquet").schema(schema).load(path)

    def readHDFSStatic(self, spark, path):
        return spark.read.format("parquet").load(path)

    def pipelineInit(self, static_df):
        sys_indexer = StringIndexer(inputCol="system", outputCol="system_hash")
        user_indexer = StringIndexer(inputCol="user", outputCol="user_hash")
        api_indexer = StringIndexer(inputCol="api", outputCol="api_hash")
        inputs = [sys_indexer.getOutputCol(), user_indexer.getOutputCol(),
                  api_indexer.getOutputCol()]
        encoder = OneHotEncoderEstimator(inputCols=inputs, outputCols=[
                                         "system_vec", "user_vec", "api_vec"])
        pipeline = Pipeline(
            stages=[sys_indexer, user_indexer, api_indexer, encoder])
        pipelineModel = pipeline.fit(static_df)
        return pipelineModel

    def modelTraining(self, model, static_df, streaming_df, k_value):

        result = model.transform(static_df)
        stream_result = model.transform(streaming_df)

        train_data, test_data = result.randomSplit([0.8, 0.2], seed=1234)
        df_train = train_data.withColumn('set', lit(0))
        df_train = df_train.withColumn('id', lit(-1))
        df_test = test_data.withColumn('set', lit(1))

        joined = df_test.union(df_train.select(*df_test.columns))

        train_data = joined.filter('set == 0')
        test_data = joined.filter('set == 1')

        train, validation = train_data.randomSplit([0.8, 0.2], seed=1234)
        pipeline = self.featureSelection()
        pipeline_model = pipeline.fit(train)

        train_transformed = pipeline_model.transform(train)
        # validation_transformed = pipeline_model.transform(validation)
        # test_transformed = pipeline_model.transform(test_data)
        stream_transformed = pipeline_model.transform(stream_result)
        kmeanModel, stream_predictions = self.kmeans(
            k=k_value, static_df=train_transformed, streaming_df=stream_transformed)
        full_stream_predictions = self.findEuclidean(
            model=kmeanModel, streaming_df=stream_predictions)
        return full_stream_predictions

    def kmeans(self, k, static_df, streaming_df):
        kmeans = KMeans(k, seed=1)  # 10 clusters here
        model = kmeans.fit(static_df.select('features'))
        # predictions = model.transform(staticDF)
        stream_predictions = model.transform(streaming_df)
        return model, stream_predictions

    def findEuclidean(self, model, streaming_df):
        columns_drop = ['system_hash', 'user_hash', 'api_hash', 'system_vec', 'user_vec', 'api_vec', 'dateFormated',
                        'hour', 'minute', 'day', 'month', 'year', 'weekday', 'weekend', 'monthbegin', 'monthend', 'features']
        centers = model.clusterCenters()
        fixed_entry = centers  # for example, the entry against which you want distances
        distance_udf = udf(lambda x, y: float(
            euclidean(x, fixed_entry[y])), FloatType())
        # For joining streaming dataframe
        streaming_df = streaming_df.withColumn('distances', distance_udf(
            col('features'), col('prediction'))).drop(*columns_drop)
        distances_benchmark = streaming_df.withWatermark("date", "1 minute").groupby('prediction', window('date', "1 minute", "1 minute")).agg(
            avg('distances').alias('avg_distances'), stddev('distances').alias('std_distances'), max('distances').alias('max_distances'))
        streaming_df = streaming_df.join(
            distances_benchmark, "prediction", "inner").drop("window")

        return streaming_df

    def featureSelection(self):
        dc = DateConverter(inputCol='date', outputCol='dateFormated')
        hrex = HourExtractor(inputCol='date')
        minex = MinExtractor(inputCol='date')
        dex = DayExtractor(inputCol='dateFormated')
        mex = MonthExtractor(inputCol='dateFormated')
        yex = YearExtractor(inputCol='dateFormated')
        wdex = WeekDayExtractor(inputCol='dateFormated')
        wex = WeekendExtractor()
        mbex = MonthBeginExtractor()
        meex = MonthEndExtractor()
        # Data process
        va = VectorAssembler(inputCols=["system_vec", "user_vec", "api_vec", 'count_req', '%diff_req', '%diff_sys', '%diff_api',
                                        '%diff_user', 'weekday', 'weekend', 'monthbegin', 'monthend', 'hour', 'minute', 'day', 'month', 'year'], outputCol="features")
        # scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
        # scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
        pipeline = Pipeline(
            stages=[dc, hrex, minex, dex, mex, wdex, wex, mbex, meex, yex, va])
        return pipeline
        
    def findAnomaly(self,stream_df):
        alert_udf = udf(lambda avg_dist, std_dist, dist: dist >=
                    avg_dist+2*std_dist, BooleanType())
        stream_alerts = stream_df.withColumn('label', alert_udf(
            col('avg_distances'), col('std_distances'), col('distances')))
        stream_alerts_broadcast = stream_alerts.select([c for c in stream_alerts.columns if c not in {
                                                        'prediction', 'distances', 'avg_distances', 'std_distances', 'max_distances', 'label'}]).where(stream_alerts.label == 1)
        return stream_alerts_broadcast


    def sendEmail(self, stream_df):
        notifier = Notifier(config=json.loads(s='''
        {
        "cases": {
            "exit_2": {
            "alert_name": "cms-htcondor-es-validation",
            "email": {
                "send_ok": true,
                "to": [
                "yanisa.sunthornyotin@cern.ch"
                ]
            },
            "entities": [
                "default entity"
            ],
            "snow": {
                "assignment_level": 3,
                "functional_element": "",
                "grouping": true,
                "service_element": "MONITORING"
            },
            "source": "cms-monit-notifier",
            "status": "ERROR",
            "targets": [
                "email",
                "snow"
            ]
            }
        },
        "default_case": {
            "alert_name": "cms-htcondor-es-validation",
            "email": {
            "send_ok": true,
            "to": [
                "yanisa.sunthornyotin@cern.ch"
            ]
            },
            "entities": [
            "default entity"
            ],
            "source": "cms-monit-notifier",
            "status": "OK",
            "targets": [
            "email"
            ]
        },
        "notification_endpoint": "http://monit-alarms.cern.ch:10011"
        }'''
                                            ))
        alert_streaming_flow = stream_df.writeStream.foreach(lambda alert: notifier.send_notification(subject=alert.system,description=json.dumps(alert.asDict(), default=str))).start()
        alert_streaming_flow.awaitTermination()

def main():
    alert = StreamingAlert()
    conf = (SparkConf()
            .setAppName("AlertingTriage-streaming"))
    sc = SparkContext(conf=conf).getOrCreate()
    spark = SparkSession.builder.appName(sc.appName).getOrCreate()
    sc.addPyFile('notifier.py')

    schema = StructType().add("window", StructType().add("start", TimestampType()).add("end", TimestampType())).add("system", StringType()).add("api", StringType()).add("user", StringType()).add("count_req", LongType()).add("req_load", LongType()).add("system_load", LongType()).add("api_load",
                                                                                                                                                                                                                                                                                      LongType()).add("user_load", LongType())        .add("avg_req", DoubleType()).add("%diff_req", DoubleType()).add("avg_sys", DoubleType()).add("%diff_sys", DoubleType()).add("avg_api", DoubleType()).add("%diff_api", DoubleType()).add("avg_user", DoubleType()).add("%diff_user", DoubleType())
    alerts_hist = alert.readHDFSStatic(
        spark=spark, path="/cms/users/carizapo/ming/fullDiff_cmsweb_logs")
    alerts = alert.readHDFSStream(
        spark=spark, schema=schema, path="/cms/users/carizapo/ming/fullDiff_cmsweb_logs")
    drop_col = ['window']
    raw_data_init = alerts_hist.withColumn(
        'date', col("window.start")).drop(*drop_col)
    stream_data = alerts.withColumn(
        'date', col("window.start")).drop(*drop_col)
    pipeline_model = alert.pipelineInit(static_df=raw_data_init)
    full_stream_predictions = alert.modelTraining(
        model=pipeline_model, static_df=raw_data_init, streaming_df=stream_data,k_value=10)
    stream_alerts_broadcast=alert.findAnomaly(stream_df=full_stream_predictions)
    alert.sendEmail(stream_df=stream_alerts_broadcast)


if __name__ == '__main__':
    main()
