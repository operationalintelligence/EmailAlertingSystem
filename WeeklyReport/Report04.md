WEEK 4 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Finished finding rolling average (1 week interval) </li><li>[x] Comparing between actual amount of email alert and rolling average</li><li>[x] Labeled the abnormal email: group of system that the difference between current count and rolling average is more that 70% of 1 week average (may change later to improve the trigger according to email fluctuation bahaviour)</li><li>[x] Finished sending abnormal alert to email (now is sent to yanisa.sunthornyotin@cern.ch for testing)</li><li>[x] Finished visualization dashboard example (hourly grouping email count)</li></ul>| <ul><li> Still finding root cause of why streaming flows keep aborting after running for a period of time</li><ul> | <ul><li>Improve static/streaming workflow</li> <li> Improve alert detection benchmark/strategies</li><li>daily/weekly visualization dashboard</li><ul> |
  
1 Hour window frame: amount of alerting email 
------------------

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/VisualizationExample.png)

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/InterestingTrendHourly.png)

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/sdEx1.png)

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/sdEx2.png)

Error report
===========
Found after running the streaming job for a long time. 
Try to retrieve daily and weekly data but the job abort before finish saving to HDFS. Now using hourly data to visualize the trend instead.
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/error_capture/streamingAbort.png)