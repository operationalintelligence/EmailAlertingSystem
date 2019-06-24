WEEK 3 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|:------------|:------------|
| <ul><li>[x] HDFS Store/Consume: Saving dataframe to HDFS in parguet form and consuming form HDFS to prepare for applying ML technique</li><li>[x] Publish alerting to Kafka</li><li>[x] Grouping field in the new Kafka topic</li></ul>| <ul><li>Multiple streaming aggregations are not supported with streaming DataFrames/Datasets so there is a limitation when we want to do a rolling average(average subject counting)</li><li>Alternative: Non-time-based windows are also not supported on streaming DataFrames/Datasets</li><ul> | <ul><li>Working on Rolling average</li><li>Get feedback from user</li><li>Study Stateful Processing in Apache Spark’s Structured Streaming</li><ul> |
    
Pattern detection API
------------------------
<li>Arbitrary Stateful Processing in Apache Spark’s Structured Streaming: Not support in Python</li>
<li>Rolling average: Use WindowSpec but streaming dataframe doesn't support windowSpec</li>
<li>Multiple aggregation: count then average doesn't support in streaming DataFrames</li>
<li>Collect in HDFS first then do another seperate aggregation: Problem with tree node makecopy</li>
    
References
----------
<li>https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html</li>