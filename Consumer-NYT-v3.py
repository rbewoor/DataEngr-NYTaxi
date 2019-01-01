# -*- coding: utf-8 -*-
"""
Created on Mon Dec 31 14:08:19 2018

@author: RB
"""
from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

print('\nStart time of consumer program: ', datetime.now().strftime("%c"))

countDocsWritten = 0
msgsReadCount = 0
topicName = 'TestNYTFullJuneButOnlyFirst12'

consumer = KafkaConsumer(
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
#     auto_offset_reset='latest',   # default is latest
     enable_auto_commit=True,
     #auto_commit_interval_ms=2000,
#     group_id='my-group1',
     consumer_timeout_ms=5000)   #consumer_timeout_ms (int) – number of milliseconds to block during 
                                 #    message iteration before raising StopIteration (i.e., ending the 
                                 #    iterator). Default block forever [float(‘inf’)].
consumer.subscribe(topicName)

client = MongoClient('localhost:27017')
collection = client.TestNYTFullJuneButOnlyFirst12Db1.TestNYTFullJuneButOnlyFirst12Col1

for message in consumer:
    msgsReadCount = msgsReadCount + 1
    NYTNewRowJsonBuiltUpAsString = "{}"
#    print(f'Full message received from Kafka::\n', message)
    msgAsString = message.value.decode("utf-8")
    if msgAsString == '':
        print('IGNORED SECOND ROW with all commans, SKIPPING to end of the for loop')
        continue
    elif msgAsString[0:8] == 'VendorID':
        print('IGNORED FIRST ROW with columns names, SKIPPING to end of the for loop')
        continue
    elif msgAsString.count(',') == 16:
        msgAsList = msgAsString.split(",")
        trip_distanceAsString = str(msgAsList[4])
        if trip_distanceAsString[0:1] == '.':
            trip_distanceAsString = '0' + trip_distanceAsString
        NYTNewRowJsonBuiltUpAsString='{' +                                      \
        '"VendorID" : ' + str(msgAsList[0]) + ', ' +                            \
        '"tpep_pickup_datetime" : ' + '"' + str(msgAsList[1]) +  '"' + ', ' +   \
        '"tpep_dropoff_datetime" : ' + '"' + str(msgAsList[2]) +  '"' + ', ' +  \
        '"passenger_count" : ' + str(msgAsList[3]) + ', ' +                     \
        '"trip_distance" : ' + trip_distanceAsString + ', ' +                   \
        '"RatecodeID" : ' + str(msgAsList[5]) + ', ' +                          \
        '"store_and_fwd_flag" : ' + '"' + str(msgAsList[6]) + '"' + ', ' +      \
        '"PULocationID" : ' + str(msgAsList[7]) + ', ' +                        \
        '"DOLocationID" : ' + str(msgAsList[8]) + ', ' +                        \
        '"payment_type" : ' + str(msgAsList[9]) + ', ' +                        \
        '"fare_amount" : ' + str(msgAsList[10]) + ', ' +                        \
        '"extra" : ' + str(msgAsList[11]) + ', ' +                              \
        '"mta_tax" : ' + str(msgAsList[12]) + ', ' +                            \
        '"tip_amount" : ' + str(msgAsList[13]) + ', ' +                         \
        '"tolls_amount" : ' + str(msgAsList[14]) + ', ' +                       \
        '"improvement_surcharge" : ' + str(msgAsList[15]) + ', ' +              \
        '"total_amount" : ' + str(msgAsList[16]) + '}'
#        print(f'JSON created for {msgsReadCount} th message::\n{NYTNewRowJsonBuiltUpAsString}')
        try:
            collection.insert_one(loads(NYTNewRowJsonBuiltUpAsString))
#            print(f"NO ERROR for {msgsReadCount} th message insert")
            countDocsWritten = countDocsWritten + 1
            if countDocsWritten % 50000 == 0:
                print(f"Successful write for {msgsReadCount} message number as {countDocsWritten} the insert into Mongo")
            if countDocsWritten % 250000 == 0:
                print(f'{countDocsWritten} th JSON builtup as=\n{NYTNewRowJsonBuiltUpAsString}')
        except Exception as e:
            print(f'Error on Insert:: {msgsReadCount} th message\nError:: ', type(e), e)
    else:
        print(f"{msgsReadCount} th message NOT having 16 commas so IGNORED MESSAGE")


print('\nExited FOR LOOP\nRead total %d messages from Kafka' %(msgsReadCount))
print('\nWritten %d documents to MongoDb' %(countDocsWritten))
print('\nDone processing. Normal exit from program at: ', datetime.now().strftime("%c"))
#
# ways to copy a dictionary value      dict2 = dict(dict1)   OR   dict2 = dict1.copy()
#