WEEK 7 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Selecting elbow point to find a proper number of cluster and apply to Kmeans</li><li>[x] Finished applying Kmeans to predict(train/test) the pattern of the amount of system logs/%Difference per WindowAVG</li><li>[x] Finished ploting system to observe the error and displaying the dataframe that show the root cause of that error</li></ul>| <ul><li>Java connection error when consume/process too much data</li></ul> | <ul><li>Improve the model efficiency and find the score of each model</li><ul> |
 
 
"system_vec","user_vec","api_vec",'count_req','%diff_req','%diff_sys','%diff_api','%diff_user'\
                                 ,'weekday', 'weekend', 'monthbegin', 'monthend','hour','minute', 'day', 'month', 'year'
Kmeans Result
------------------
Features:
<li>System name</li>
<li>User name</li>
<li>API name</li>
<li>Total amount of request per user per system in window time interval</li>
<li>%Difference of request compare to window average</li>
<li>%Difference of system call compare to window average</li>
<li>%Difference of API call compare to window average</li>
<li>%Difference of amount of user compare to window average</li>
<li>Weekday/Weekend/Month begin/Month end</li>
<li>Hour/Minute/Day/Month/Year</li>

By using 'OneHotEncoding' to tranform catagorical data(System/User/API) into numerical data, we got the new column which contain sparse vector of that particular value.


Find k value by applying elbow method (2000 samples of request)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/2000samples_noScaler.png)
According to the plotting result, we choose k=7.

CouchDB anomaly detection
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/couchdb_k7.png)


Crapserver anomaly detection
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/crabserver_k7.png)


DQM anomaly detection
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/dqm_k7.png)

