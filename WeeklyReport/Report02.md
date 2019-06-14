WEEK 2 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Data aggregation: Kafka subject realtime groupping according to timeframe(windowing)</li><li>[x] Consume message from Kafka topic ("email_alert")</li><li>[x] Define UDF in order to optimize the function processing time while streaming</li></ul>| <ul><li> Still finding root cause of why it takes time to grouping subject name. Maybe lack due to spark SQL when displaying query result or the grouping algorithm itself</li><ul> | <ul><li>Saving realtime dataframe to HDFS in order to process next (training data)</li> <li> Sending alert to kafka topic("snow_ticket") in order to send an email but still don't know how to get feedback from user</li><ul> |

Suggested Library : Spark Structured Streaming    
--------------
Structured Streaming is the Apache Spark API that lets you express computation on streaming data in the same way you express a batch computation on static data. The Spark SQL engine performs the computation incrementally and continuously updates the result as streaming data arrives.

References
----------
<li>https://docs.databricks.com/spark/latest/structured-streaming/index.html</li>
<li>https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html#api-using-datasets-and-dataframes</li>
<li>https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html</li>