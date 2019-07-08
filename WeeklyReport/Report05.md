WEEK 5 REPORT
==============
Yanisa Sunthornyotin (Ming) - Summer Student - CMS Monit Team

|        Task        |  Problem  | Next Step  | 
|:--------|------------| ------------|
| <ul><li>[x] Finished applying STLDecompose to detect the pattern of the system logs</li><li>[x] Finished applying another alternative method to normalized %difference (using Normalizer from spark MLLib)</li><li>[x] Transform new normalized data to the dataframe schema that can be broadcast via Kafka</li><li>[x] Studying about RNN to do a sequential pattern detection</li></ul>| Work on progress, no problem is found so far | <ul><li> Continue working on RNN to do a sequential pattern detection (Detect outlier)</li><li> Visualize the difference before and after apply each method</li><ul> |

STLDecompose Result
------------------
Based on this particular system logging information 
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/InterestingPattern01.png)
<li>1st row: showing %difference of the amount of system logging coming to the kafka topic compare to previous one</li>
<li>2nd row: showing %difference of the amount of system logging coming to the kafka topic compare to previous window average value</li>
<li>3rd row: showing counting result of the amount of system logging coming to the kafka topic</li>

We found that, among many systems there're some that have its own pattern (found just 1 system so far)

By using %Difference data
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/STLDecompose01.png)

By using raw data (amount of email logs coming in)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/STLDecompose02.png)


Normalizer Result
------------------
Ref:
<li>https://spark.apache.org/docs/latest/ml-features.html#normalizer</li>
Normalize data points (%Difference which is now range from negative value (i.e -0.84172 or -84.17%) to positive value (i.e 3.0381 or 303.8%)) within a time interval to be within range -1 to 1.

## How to find outlier in normalized data? 

In this case, 1.0 refers to the maximum value in that array. I think it just normalized those data to be more restrictive within the 0-1 interval. However, normally there'll be one peak (i.g. 200% or 400%) in those array of data (within 5 hours interval).

What I originally concerned with the former data (% difference) is that there'll be some system that its peak reaches 400% while some system reaches only to 100%. Each system has different behavior so I try to find another way to optimize and normalize it. We can set a certain benchmark and it will do their job better than before. For example,

Originally, we set outlier benchmark>70% that will trigger an alert that ranges from 71% and, maybe, to 400% which is not quite effective since some system, 400% considered as peak, not 150%. Some system 150% is its only peak and there's no more than that. In this case, we would trigger just 400% in the first case and trigger 150% in the second case. So you can see that setting a benchmark to 70% is not that effective.
While the newer approach, we normalize it to be in a certain range (0 to 1) whatever which system it is. In this case, the system with 400% peak will be considered as 1 and so do the system with 150% peak. So we can just set the outlier factor to be 1 and we can find the peak value which has the potential to be an abnormal email from that.

## Result from Normalizer
Here's the result from normalizer (new_label/normFeatures_list) compared to the former alerting trigger(label/features_list). The sequence data from that particular period(%difference before normalized) is represented by the 'features' column. The former benchmark I set it at %difference > 300% (features_list>3.0) and the newer one I set it at >90% (normFeatures_list>0.9).
0= No alert required
1= Should trigger an alert
features_list = The spike of particular %diff
normFeatures_list = The spike of normalized %diff

In the first picture, I think the newer approach gives us more appropriate alert triggering than the previous approach, for example, the first row(index=0/system=wmstatserver) features_list %diff reach 306%, the previous approach will consider it as an abnormal peak but the newer one will know that 306%, in this case, is not as weird as 850%(one of data point within 'features'). So it shouldn't trigger an alert at 306% peak.

This normalized data will help us configure the benchmark easier but there are still some cons. If in that time interval there's fewer datapoint with a low rate of %diff fluctuation, the normalizer will amplify each data point to almost 1.0 so even a low %diff will trigger alert too.

label(alert) vs new_label(normal)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/NormalizerResult01.png)
label(normal) vs new_label(alert)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/NormalizerResult02.png)

Full result before&after applying Normalizer within certain time interval(5 hours)
![alt text](https://github.com/operationalintelligence/EmailAlertingSystem/blob/master/screenshots/NormalizerFullResult.png)

