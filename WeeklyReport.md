WEEKLY REPORT : **Alerts Automatic Triage**
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|Week|        Task        |  Problem  | Next Step  | 
|:----:|:--------|------------| ------------|
| 1     | <ul><li>[x] Data aggregation: Kafka subject realtime groupping according to timeframe(windowing)</li><li>[x] Consume message from Kafka topic ("email_alert")</li></ul>| <ul><li>SparkContext takes time to restart so it will affect debugging process</li> <li> Email didn't keep coming constantly everyday(there's no email coming today)so it will be hard to test the result</li><ul> | <ul><li>Saving realtime dataframe to HDFS in order to process next (training data)</li> <li> Sending alert to kafka topic in or deer to send an email("snow_ticket") but still don't know how to get feedback from user</li><ul> |
