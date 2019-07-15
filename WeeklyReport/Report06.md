WEEK 6 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Finished applying LSTM to predict(train/test) the pattern of the amounth of system logs</li><li>[x] Finished ploting each system to observe the error and find MSE of the model of each system</li></ul>| <ul><li>Error while connecting to Spark cluster: reached the maximum number of parallel Spark connections. May have to refactor again</li><li>Problem with collecting streaming dataframe: temporary solved by convert to Pandas</li></ul> | <ul><li> Continue working on RNN to do a sequential pattern detection (Detect outlier), apply Standard Deviation to find outlier</li><li>Apply LSTM with %different data</li><li>Visualize the difference before and after apply each method</li><ul> |
 
 

LSTM Result
------------------
Based on this particular system logging information 
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/InterestingPattern01.png)

By using %Difference data
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/STLDecompose01.png)

By using raw data (amount of email logs coming in)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/STLDecompose02.png)

