WEEK 2 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Data aggregation: Group out unique subject name within same window to find a way to implementing pattern recognition </li><li>[x] Pattern recognition: Study about RegX on Spark Streaming</li><li>[x] Roughly sketch of the component diagram and sequence diagram of the system</li></ul>| <ul><li> Still finding root cause of why it takes time to grouping subject name. Maybe lack due to spark SQL when displaying query result or the grouping algorithm itself</li><ul> | <ul><li>Working on regX(Regular Expression)</li> <li> Sending alert to kafka topic("snow_ticket") in order to send an email but still don't know how to get feedback from user</li><ul> |
  
Component Diagram
------------------
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/ComponentDiagram.jpg "Component Diagram")

Sequence Diagram
------------------
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/SequenceDiagram.jpg "Sequence Diagram")

Suggested Library : Spark Structured Streaming    
--------------
Structured Streaming is the Apache Spark API that lets you express computation on streaming data in the same way you express a batch computation on static data. The Spark SQL engine performs the computation incrementally and continuously updates the result as streaming data arrives.

References
----------
<li>https://docs.databricks.com/spark/latest/structured-streaming/index.html</li>
<li>https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html#api-using-datasets-and-dataframes</li>
<li>https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html</li>