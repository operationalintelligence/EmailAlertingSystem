# EmailAlertingSystem
 Alerts Automatic Triage: The objective of this project is to develop a tool that detects patterns on streams  of  data,  process  complex  events  in  order  to  trigger  timely  and  relevant alerts, and automatically triage issues sending notifications to the proper channel.
 
Introduction
------------
 The CMS experiment depends on several services that should be monitored on near real time. These systems generate logs from which alerts should be triggered, however, trigger alerts for single events will give an overwhelming number of false positives. 

The objective of this project is to develop a tool that detects patterns on streams of data, process complex events in order to trigger timely and relevant alerts, and automatically triage issues sending notifications to the proper channel.

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/Diagram/MonitoringArchitecture.png "Monitoring Architecture")

There are several different data producers and delivery mechanisms. For the scope of this project, we'll consume messages only from Apache Kafka.