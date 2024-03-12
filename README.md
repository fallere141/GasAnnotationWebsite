# gas-framework
An enhanced web framework (based 
on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Archive Process

1. When run.py finish annotating, it will doublecheck if it's a free user, if it is a free user, `run.py` will put it into the `zhangzx-Glacier` SNS
2. `zhangzx-Glacier` SNS is subscribed by a delay queue from SQS called `zhangzx-Glacier`, and the latency is set to 5 minutes.
3. After 5 minutes, the `archive.py` will receive the message and update the lifecycle of the result of this job, which is going to change this S3 object class in to glacier class in 0 day, and delete this message.

### Reason

Using a delay queue, not set delay to message, so that I can change the setting directly by amazon console, rather than change the code in server, which is more clumsy.
Lifecycle is now a suggestion way of Amazon to deal with different kinds of object with different accessibility.

## Restore Process

1. When user subscribes, web server will send a message to `zhangzx-glacier-restore` with the user's id.
2. `restore.py` will get the user_id and query it in dynamodb to get all jobs from this user.
3. If the job is already finished, `restore.py` will update it in dynamodb to be a premium user, start restore job and send a message to `zhangzx-glacier-thaw` with this job's job_id.
4. If the job is still annotating , `restore.py` will update it in dynamodb to be a premium user. When the annotating finished, `run.py` will failed in doublecheck and not put it into `zhangzx-Glacier` queue and archive it in glacier.
5. And finally delete the message of user_id
6. `thaw.py` will receive message of job_id whose result file is restoring.
7. 'thaw.py' will check job_id in the message, if the corresponding result file is not restored yet, it will ignore and continue check next job_id.
8. if the corresponding result file has restored, it will copy it in place, but in standard class, which will also remove its lifecycle. And finally delete this message with job_id.

### Reason
Since restore job will be start on util server but asked from web server. I have to use SNS and SQS to communicate with each other. And restore job is request to Amazon and will not be finished immediately. In order to not block other restore job, a best way is to set another SNS and SQS pair to handle this job in `thaw.py` and when it finished, I can continue to finish this restore job. So that, every thing can be done async without block.

## Web auto scale

### Observe
When I start the ddos test, after a little more than one minute, the autoscale group starts to init a new server. And then I closed the test, Autoscale group will terminate one web server in about 5-6 minutes.

### Explain

#### Scale Out 
When I start the test, the http 2xx request will suddenly increase to more than 200 (actually more that thousands) and after the average of http 2xx request move that 200 more than one minute, the cloud metric will alarm, and when the autoscale group receive this alarm, it will start to add one more server, and wait 300 seconds to decided what to do next. 

#### Scale In
At the same time, the target response is never larger than 0.01, and keeps alarming. Since the minimal number of the server is 2, the autoscale group received this alarm but will not terminate extra server, when there are only two server. However, when we are testing autoscale ability. There are more then 2 server, after the 300 seconds waiting, autoscale group will terminate one of the web server, because of this cloud metric alarm.(actually it will not always alarm, since it will become insufficient data, when there are actually no http request. But when there is a request, I think it will start alarm, I guess.)

Here are some screenshots:
![Pasted Graphic.png](Pasted%20Graphic.png)
![Pasted Graphic 2.png](Pasted%20Graphic%202.png)
![Pasted Graphic 3.png](Pasted%20Graphic%203.png)

## Ann auto scale
After I start the test about 5 minutes, the Ann autoscale group add a unit and then I stopped the test. Then after about 6 minutes, the server terminate.

### Scale out
When I start the script to send a lot of fake job request to `zhangzx_job_requests`, there will be increasing NumberOfMessagesSent. After 10 minutes, the cloud metric will alarm, and ann autoscale group will add a new unit.

### Scale in
Same as Scale in from web scale in. Before I start the test script, it will keep alarming because NumberOfMessagesSent is always under 5. Since the minimal server number is 2, it will not terminate extra server. However, when there is more server in autoscale group at autoscale group is not in 300 seconds waiting, it will reduce a server in the autoscale group.

Here are some screenshots:
![Pasted Graphic 4.png](Pasted%20Graphic%204.png)
![Pasted Graphic 5.png](Pasted%20Graphic%205.png)
![Pasted Graphic 6.png](Pasted%20Graphic%206.png)