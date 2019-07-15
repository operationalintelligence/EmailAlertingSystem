WEEK 6 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Finished applying LSTM to predict(train/test) the pattern of the amount of system logs</li><li>[x] Finished ploting each system to observe the error and find MSE of the model of each system</li></ul>| <ul><li>Error while connecting to Spark cluster: reached the maximum number of parallel Spark connections. May have to refactor again</li><li>Problem with collecting streaming dataframe: temporary solved by convert to Pandas</li></ul> | <ul><li> Continue working on RNN to do a sequential pattern detection (Detect outlier), apply Standard Deviation to find outlier</li><li>Apply LSTM with %different data</li><li>Convert predicted data back to spark dataframe, correct datetime column and dealing with scaler</li><ul> |
 
 

LSTM Result
------------------
PopDB: amount of system logging prediction
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/LSTM_popdb.png)

DBS: amount of system logging prediction
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/LSTM_dbs.png)
<li>The lowest plot indicates the original data before applying MinMaxScaler. As you can see, there'll be a little bit of graph shifting</li>

ConfDB: amount of system logging prediction
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/LSTM_confdb.png)
<li>The lowest plot indicates the original data before applying MinMaxScaler. As you can see, there'll be a little bit of graph shifting</li>
