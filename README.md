# DataEngr-NYTaxi
As of now I have only managed to successfully read in from the ORGINAL unedited file that comes from NYT website. No opening of
the file and editing in anyway. I found opening and editing in excel messes it up somehow.
The final codes for the producer and consumer are the files: Consumer-NYT-v3.py  and  Producer-NYT-v3.py

JUPYTER PRODUCER: There are three version of producer runs for sending to Kafka:
1st file NYTaxi-Prod-ReadFullFile-SendAll-8713833rows.ipynb reads everything and sends
2nd file NYTaxi-Prod-ReadFullFile-Send24hrs316569rows.ipynb reads a set number of records specified coz I opened in excel and saw
visually up to what point counts as 01-Jun-18 00:00:00 to 23:59:59 for first 24 hours
3rd file NYTaxi-Prod-ReadFullFile-SendfFrst12rows.ipynb is to send only small set for testing

JUPYTER CONSUMER:
file NYT12RecsReadnWrite3.ipynb has the detailed print statements of how the processing happens for first 12 messages successfully.
Make sure YOU CAREFULLY LOOK AT print statement supression before running OR U'LL CRASH YOUR SYSTEM for the full data.

JUPYTER ANALYSIS:
file Analysis-NYT-v3-20180102-0105.ipynb
First cell has run on test data for 12 timeslots of 5 mins each thus covering one hour.
Second cell has changed code to run on larger 24 hours worth of data for 24 timeslots of 1 hour each thus covering the full day.
Code currently only calculates for the matrix and populates it. But for the LIST (location based counts) is still pending.
Problem explained below about check to not miss out any records from original csv that do fall into the 01-June date.


PROBLEM FOUND with the way the first 24 hours was read. Simply reading first 316569 messages (discarding the first two as header and blank info) has inserted 316567 docs into mongo. But the dates are outside the PUTime of gte=2018-06-01 00:00:00 and lt=2018-06-02 00:00:00. Only 316188 docs meet this condition. Will need to change consumer to check the date range before proceeding to insert (readability below is bad but see screenshot image DateCheckb4InsertReq-20190101.jpg)


> db.TestNYTFullJuneButOnly24hrsCol1.count()
316567
> db.TestNYTFullJuneButOnly24hrsCol1.find({},{_id:0, tpep_pickup_datetime: 1, tpep_dropoff_datetime: 1}).sort({tpep_pickup_datetime: 1}).limit(5)
{ "tpep_pickup_datetime" : "2008-12-31 13:51:38", "tpep_dropoff_datetime" : "2008-12-31 14:21:38" }
{ "tpep_pickup_datetime" : "2009-01-01 17:22:13", "tpep_dropoff_datetime" : "2009-01-01 18:05:13" }
{ "tpep_pickup_datetime" : "2018-05-31 06:08:31", "tpep_dropoff_datetime" : "2018-05-31 06:18:15" }
{ "tpep_pickup_datetime" : "2018-05-31 06:33:15", "tpep_dropoff_datetime" : "2018-05-31 06:35:36" }
{ "tpep_pickup_datetime" : "2018-05-31 06:40:08", "tpep_dropoff_datetime" : "2018-05-31 06:45:04" }
> db.TestNYTFullJuneButOnly24hrsCol1.find({},{_id:0, tpep_pickup_datetime: 1, tpep_dropoff_datetime: 1}).sort({tpep_pickup_datetime: -1}).limit(5)
{ "tpep_pickup_datetime" : "2018-06-02 23:49:30", "tpep_dropoff_datetime" : "2018-06-03 00:00:35" }
{ "tpep_pickup_datetime" : "2018-06-02 23:42:15", "tpep_dropoff_datetime" : "2018-06-02 23:55:21" }
{ "tpep_pickup_datetime" : "2018-06-02 23:39:02", "tpep_dropoff_datetime" : "2018-06-03 00:05:32" }
{ "tpep_pickup_datetime" : "2018-06-02 23:24:21", "tpep_dropoff_datetime" : "2018-06-02 23:38:11" }
{ "tpep_pickup_datetime" : "2018-06-02 23:16:16", "tpep_dropoff_datetime" : "2018-06-02 23:21:04" }
> db.TestNYTFullJuneButOnly24hrsCol1.find( {"tpep_pickup_datetime": {$gte: "2018-06-01 00:00:00", $lt: "2018-06-02 00:00:00"}} ).count()
316188
> db.TestNYTFullJuneButOnly24hrsCol1.find().count()
316567

