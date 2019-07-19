WEEK 7 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Finished applying Kmeans to predict(train/test) the pattern of the amount of system logs/%Difference per WindowAVG</li><li>[x] Finished ploting system to observe the error and displaying the dataframe that show the root cause of that error</li></ul>| <ul><li>Java connection error when consume/process too much data</li></ul> | <ul><li>Improve the model efficiency and find the score of each model</li><ul> |
 
 

Kmeans Result
------------------
PopDB: amount of system logging prediction
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/LSTM_popdb.png)


DBS: amount of system logging prediction
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/LSTM_dbs.png)
<li>The lowest plot indicates the original data before applying MinMaxScaler. As you can see, there'll be a little bit of graph shifting</li>


ConfDB: amount of system logging prediction
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/LSTM_confdb.png)
<li>The lowest plot indicates the original data before applying MinMaxScaler. As you can see, there'll be a little bit of graph shifting</li>
