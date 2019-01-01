# -*- coding: utf-8 -*-
"""
Created on Tue Jan  1 02:26:44 2019

@author: RB
"""
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
import csv

print('\nStartTime is:',datetime.now().strftime("%c"))
fileLocation = r'C:\Everything\01SRH-BDBA Acads\Blk2-DataEngr\NYTaxi\testingData\yellow_tripdata_2018-06-full.csv'
topicName = 'TestNYTFullJune'
producer = KafkaProducer(bootstrap_servers='localhost:9092')
rowsRead = 0
with open(fileLocation, "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    for row in csv_reader:
        rowsRead = rowsRead + 1
        joinedRow = ','.join(row)
        producer.send(topicName, joinedRow.encode('utf-8'))
        if rowsRead % 50000 == 0:
#            print(f'Row={rowsRead}, Contents are:::\n{joinedRow}' )
            print(f'Processed the {rowsRead} th row of csv and sent to Kafka' )
# =============================================================================
#         if rowsRead == 316569:
#             print('Reached limit for first 24 hours and exiting last entry should have PickUp time of 01-06-2018  23:43:00')
#             break
# =============================================================================

print(f"\n\nProcessed {rowsRead} rows and sent")
print('\nEndTime is:',datetime.now().strftime("%c"))