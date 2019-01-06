# -*- coding: utf-8 -*-
"""
Created on Sun Jan  6 11:35:20 2019

@author: RB
"""
from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

print('\nStart time of consumer program: ', datetime.now().strftime("%c"))

countDocsWritten1 = 0
countDocsWritten2 = 0
msgsReadCount = 0
topicName = 'TestNYTFullJuneDb10

consumer = KafkaConsumer(
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
#     auto_offset_reset='latest',   # default is latest
     enable_auto_commit=True,
     auto_commit_interval_ms=2000,
     group_id='my-group1')
#     consumer_timeout_ms=5000)   #consumer_timeout_ms (int) – number of milliseconds to block during 
                                 #    message iteration before raising StopIteration (i.e., ending the 
                                 #    iterator). Default block forever [float(‘inf’)].
consumer.subscribe(topicName)

client1 = MongoClient('localhost:27017')
client2 = MongoClient('localhost:27017')
collection1 = client1.TestNYTFullJuneDb10.TestNYTFullJuneColJun01to12
collection2 = client2.TestNYTFullJuneDb10.TestNYTFullJuneColJun13to24

boundLowerPUDatetime1 = '2018-06-01 00:00:00'   # the pickup datetime from csv to be greater or equal to this
boundUpperPUDatetime1 = '2018-06-02 00:00:00'   # the pickup datetime from csv to be lower than this
boundLowerPUDatetime2 = '2018-06-02 00:00:00'   # the pickup datetime from csv to be greater or equal to this
boundUpperPUDatetime2 = '2018-06-10 00:00:00'   # the pickup datetime from csv to be lower than this

for message in consumer:
    msgsReadCount = msgsReadCount + 1
    NYTNewRowJsonBuiltUpAsString = "{}"
#    print(f'Full message received from Kafka::\n', message)
    msgAsString = message.value.decode("utf-8")
    if msgAsString == '':
        print('IGNORED SECOND ROW with all commans, CONTINUING to end of the for loop')
        continue
    elif msgAsString[0:8] == 'VendorID':
        print('IGNORED FIRST ROW with columns names, CONTINUING to end of the for loop')
        continue
    elif msgAsString.count(',') == 16:
        msgAsList = msgAsString.split(",")
        trip_distanceAsString = str(msgAsList[4])
        if trip_distanceAsString[0:1] == '.':        # a 0 value comes as  .00  so checking and making as 0.00
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
        #print(f'Msg {msgsReadCount} - JSON built as=\n{NYTNewRowJsonBuiltUpAsString}')
        if ( str(msgAsList[1]) < boundUpperPUDatetime1 and str(msgAsList[1]) >= boundLowerPUDatetime1 ):
            try:
                collection1.insert_one(loads(NYTNewRowJsonBuiltUpAsString))
                #print(f"Collection 1 -- NO ERROR for {msgsReadCount} th message insert")
                countDocsWritten1 = countDocsWritten1 + 1
                if countDocsWritten1 % 50000 == 0:
                    print(f"Collection 1 - write SUCCESS - {msgsReadCount} th message as {countDocsWritten1} th insert")
                if countDocsWritten1 % 250000 == 0:
                    print(f'Msg {msgsReadCount} - JSON built as=\n{NYTNewRowJsonBuiltUpAsString}')
            except Exception as e:
                print(f'Collection 1 ERROR Insert :: {msgsReadCount} th message\nError:: ', type(e), e)
        elif ( str(msgAsList[1]) < boundUpperPUDatetime2 and str(msgAsList[1]) >= boundLowerPUDatetime2 ):
            try:
                collection2.insert_one(loads(NYTNewRowJsonBuiltUpAsString))
                #print(f"Collection 2 -- NO ERROR for {msgsReadCount} th message insert")
                countDocsWritten2 = countDocsWritten2 + 1
                if countDocsWritten2 % 50000 == 0:
                    print(f"Collection 2 - write SUCCESS - {msgsReadCount} th message as {countDocsWritten2} th insert")
                if countDocsWritten2 % 500000 == 0:
                    print(f'Msg {msgsReadCount} - JSON built as=\n{NYTNewRowJsonBuiltUpAsString}')
            except Exception as e:
                print(f'Collection 2 ERROR Insert :: {msgsReadCount} th message\nError:: ', type(e), e)
    else:
        print(f"{msgsReadCount} th message NOT having 16 commas so IGNORED MESSAGE")


print('\nExited FOR LOOP\nRead total %d messages from Kafka' %(msgsReadCount))
print('\nWritten %d documents to Mongo Collection1' %(countDocsWritten1))
print('\nWritten %d documents to Mongo Collection2' %(countDocsWritten2))
print('\nDone processing. Normal exit from program at: ', datetime.now().strftime("%c"))
#
# ways to copy a dictionary value      dict2 = dict(dict1)   OR   dict2 = dict1.copy()
#