WEEK 8 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] KMeans model evaluation</li><li>[x] Definding new alert benchmark: distance from each datapoint to the center of cluster</li><li>[x] Publish new email notifier dataflow based on predicted dataframe</li></ul>|<ul></ul> | <ul><li>Document all project progress/Future work</li><ul> |
 
 
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
 
Model Evaluation
------------------
Contrary to supervised learning where we have the ground truth to evaluate the model’s performance, clustering analysis doesn’t have a solid evaluation metric that we can use to evaluate the outcome of different clustering algorithms. Moreover, since kmeans requires k as an input and doesn’t learn it from data, there is no right answer in terms of the number of clusters that we should have in any problem. In the cluster-predict methodology, we can evaluate how well the models are performing based on different K clusters since clusters are used in the downstream modeling.
2 metrics that may give us some intuition about k:

<li>Elbow method</li>

Elbow method gives us an idea on what a good k number of clusters would be based on the sum of squared distance (SSE) between data points and their assigned clusters’ centroids. We pick k at the spot where SSE starts to flatten out and forming an elbow. We’ll use the geyser dataset and evaluate SSE for different values of k and see where the curve might form an elbow and flatten out.
Find k value by applying elbow method (2000 samples of request)

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/ElbowNoScaler.png)
According to the plotting result, we choose k=10.

<li>Silhouette analysis</li>

![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/Silhouette.png)


Silhouette: Study the separation distance between the resulting clusters
<li>+1 indicate that the sample is far away from the neighbouring clusters</li>
<li>0 indicates that the sample is on or very close to the decision boundary between two neighbouring clusters</li>
<li>-1 indicate that those samples might have been assigned to the wrong cluster</li>

New Alert Benchmark
========
Based on the distance from the datapoint to the center of its cluster. If the distance is more than 2 standard deviation of the mean distance in each cluster, that datapoint should be considered as an outlier.

CouchDB anomaly detection(>2sd)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/couchdb_2sd.png)

CouchDB anomaly detection(>1sd)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/couchdb_1sd.png)

CouchDB anomaly detection(max distance +/-40%)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/couchdb_out40%25.png)


DQM anomaly detection(>1sd)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/dqm_1sd.png)


DQM anomaly detection(>2sd)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/dqm_2sd.png)


Crabserver anomaly detection
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/crabserver_1sd.png)


Dataframe for finding the root cause of the error
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/rootcause_df.png)



