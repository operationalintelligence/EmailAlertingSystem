WEEK 1 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Data aggregation: Kafka subject realtime groupping according to timeframe(windowing)</li><li>[x] Consume message from Kafka topic ("email_alert")</li><li>[x] Define UDF in order to optimize the function processing time while streaming</li></ul>| <ul><li> Taking too much time choosing a library and find the right one that is appropriate to our usecase</li><li> Email didn't keep coming constantly everyday(there's no email coming today/even if it comes, it's not coming all the time) so it will be hard to test the result</li><ul> | <ul><li>Saving realtime dataframe to HDFS in order to process next (training data)</li> <li> Sending alert to kafka topic("snow_ticket") in order to send an email but still don't know how to get feedback from user</li><ul> |

Choosing Library
--------------
|Lib/Stack|        Pros        |  Cons  |
|:----:|:--------|------------| 
| kafka-python     | <ul><li>Easy to use, debug, and restart the process</li><li>Easy to unwrap kafka object and deal with the dataframe</li></ul>| <ul><li>May not compatible with Spark/HDFS</li><li> Cannot process data in realtime just only in static way</li><ul> |
| pykafka     | <ul><li>Easy to use, debug, and restart the process</li><li>Easy to unwrap kafka object and deal with the dataframe</li></ul>| <ul><li>May not compatible with Spark/HDFS</li><li> Cannot process data in realtime just only in static way</li><li>Easy to unwrapped kafka object and deal with the dataframe</li><li>Support large amount of producing transaction more than consuming transaction</li><ul> |
| Spark Structured Streaming     |  <ul><li> Definitely compatible with Spark/HDFS</li><li>Easy to use, debug, and restart the process</li><li> Can process data in realtime</li><li>Easy to unwrap kafka object and deal with the dataframe</li><ul> |<ul><li> Still working on exploring whether it can trigger sending data on the aggregation value or not</li></ul>| 
| Spark Streaming     |  <ul><li> Definitely compatible with Spark/HDFS</li><li> Can process data in realtime</li><ul> |<ul><li>Difficult to use, debug, and restart the process</li><li>Might be more complex to unwrapped kafka object and deal with the dataframe</li></ul>| 
    
Suggested Library : Spark Structured Streaming    
--------------
Structured Streaming is the Apache Spark API that lets you express computation on streaming data in the same way you express a batch computation on static data. The Spark SQL engine performs the computation incrementally and continuously updates the result as streaming data arrives.
Ref: 
<li>https://docs.databricks.com/spark/latest/structured-streaming/index.html</li>
<li>https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html#api-using-datasets-and-dataframes</li>
<li>https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html</li>