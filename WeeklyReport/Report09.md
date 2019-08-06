WEEK 9 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Upload/setup code on CERN lxplus</li><li>[x] Refactor code and seperate 2 email sending strategies: semi-static(must join with streaming dataframe later to publish email) and pure streaming dataframe</li><li>[x] Publish new email notifier dataflow based on predicted dataframe</li></ul>|<ul>Cannot install spark MLLib dependency (com.github.fommil.netlib/Native BLAS) in the lxplus/bash command</ul> | <ul><li>Test the system</li><li>Find the solution of how to install spark MLlib on lxplus</li><li>Document all project progress/Future work</li><ul> |
 

Alerting Triage System files
 ==========
 <li>MonitStreaming.py: consume all kafka message, apply primary grouping with window function and collect in HDFS. Should be running all the time(start just once)</li>
 <li>MultipleAgg.py: Do a multiple aggregation on the kafka data that will be consumed from HDFS. In this module, aggregation must be apply to static dataframe not streaming dataframe(Unless the structured streaming support Multiple aggregation in the future). This file must be rerun again in every desired period(daily, weekly, etc.)</li>
<li>KmeansAlertEmail-Static.py: One of 2 types of email publishing and ML pipeline. This file will process and predict static dataframe then join with streaming dataframe later in order to publish email</li>
<li>KmeansAlertEmail-Streaming.py:One of 2 types of email publishing and ML pipeline. This file will process and predict streaming dataframe based on model created by static dataframe in the first run. It doesn't have to be joined with any other streaming dataframe but can be published to the user email right away (Possibly run just once in the first time the system is started)</li>