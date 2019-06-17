WEEK 2 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Data aggregation: Group out unique subject name within same window to find a way to implementing pattern recognition </li><li>[x] Pattern recognition: Study about RegExr on Spark Streaming</li><li>[x] Roughly sketch of the component diagram and sequence diagram of the system</li></ul>| <ul><li> Still finding root cause of why it takes time to grouping subject name. Maybe lack due to spark SQL when displaying query result or the grouping algorithm itself</li><ul> | <ul><li>Working on RegExr(Regular Expression)</li> <li> Sending alert to kafka topic("snow_ticket") in order to send an email but still don't know how to get feedback from user</li><ul> |
  
Component Diagram
------------------
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/ComponentDiagram.jpg "Component Diagram")


## module
### Kafka Module
* __Producer ’snow_ticket’__: Publish message to ask for user feedback
* __Producer ’filter_alert’__: Alert user about abnormal behaviour of email flow
* __Email flow ’email_alert’__: Consume real data streaming of user email alert
* __Mock Email flow ’mock_email_alert’__: Consume mock data streaming of user email alert
* __User feedback__: Consume feedback data streaming to improve the efficiency of dynamic alerting 
Streaming engine
* __Unwrapping object__: Unwrap raw data from kafka consumer and structurize the object in to email subject, timestamp, and content.
* __Pattern/Aggregation module__: Detecting, categorising and grouping frequent email as well as the sequence pattern of the email flow
* __Output stream__: Publish refined alerting email to user
### HDFS
Collect the Data frame of prioritised email and grouped email
Prioritising module: Determine the importance of each email group or sequence
### ML module
Train and test the model to determine the priority of  email group or sequence. Update existing data priority in prioritising module
### Feedback module
Receive raw user feedback data. Clean and unwrap data out in order to user it for ML technique(email classification).


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