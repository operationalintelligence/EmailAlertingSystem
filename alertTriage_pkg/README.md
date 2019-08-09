# AlertTriage Package
 
Main File
------------

***MonitStreaming.py***
 Stream Kafka message(CMSWeb_log) into the HDFS path name `/cms/users/carizapo/ming/data_cmsweb_logs` and apply window grouping

**API**

<li>consume(spark,schema,server_url): streaming_df</li>

<li>windowGrouping(raw_data,watermark,time_interval,updated_time): streaming_df</li>

<li>startMonit(streaming_df,hdfs_path,checkpoint_location): DataStreamWriter</li>
<li>stop(streaming_df): void</li>


___
***MultipleAggregation.py*** 
Consume message generated from MonitStreaming.py (HDFS:`/cms/users/carizapo/ming/data_cmsweb_logs`) and do multiple aggregation to find rolling average and other significant parameters then collect in another HDFS path name `/cms/users/carizapo/ming/fullDiff_cmsweb_logs`

**API**

<li>readHDFSStream( spark, schema, path): streaming_df</li>

<li>readHDFSStatic( spark, path): static_df</li>

<li>defineGrouping(static_df,observer_period,mean_period): static_df</li>

<li>startAggregation(stream_df,static_df,hdfs_path,checkpoint_path): DataStreamWriter</li>

___
***StreamingAlert.py***
Consume data from MultipleAggregation.py(HDFS: `/cms/users/carizapo/ming/fullDiff_cmsweb_logs`), prepare for machine learning model by applying One-hot encoder, input k-value for k-means and predict the anomally by clustering. Determine outlier with standard deviation then publish alert to email using notifier.

**API**

<li>readHDFSStream(spark, schema, path): streaming_df</li>

<li>readHDFSStatic(spark, path): static_df</li>

<li>pipelineInit(static_df): Pipeline</li>

<li>modelTraining( model, static_df, streaming_df, k_value): streaming_df</li>

<li>kmeans(k, static_df, streaming_df): model, streaming_df</li>

<li>findEuclidean(model, streaming_df): streaming_df</li>
<li>featureSelection(): pipeline</li>
<li>findAnomaly(stream_df): streaming_df</li>