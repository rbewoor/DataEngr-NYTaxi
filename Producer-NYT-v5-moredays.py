# -*- coding: utf-8 -*-
"""
Created on Sun Jan  6 11:38:51 2019

@author: RB
"""
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
import csv
import math

print('\nStartTime is:',datetime.now().strftime("%c"))
fileLocation = r'C:\Everything\01SRH-BDBA Acads\Blk2-DataEngr\NYTaxi\testingData\yellow_tripdata_2018-06-full.csv'
topicName = 'TestNYTFullJuneDb10
producer = KafkaProducer(bootstrap_servers='localhost:9092')
maxRows2Read = 10000000                            # Specify the maximum number of rows to read from the csv
rowsReadcount = 0
countSent2Producer = 0
boundMatchRowsCount = 0
boundNotMatchRowsCount = 0
boundLowerPUDatetime = '2018-06-01 00:00:00'   # the pickup datetime from csv to be greater or equal to this
boundUpperPUDatetime = '2018-06-11 00:00:00'   # the pickup datetime from csv to be lower than this

with open(fileLocation, "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    for row in csv_reader:
        rowsReadcount = rowsReadcount + 1
        if rowsReadcount % 500000 == 0:    
            print(f'Processing csv row number {rowsReadcount}')
        if rowsReadcount == (maxRows2Read + 1):
            print('Reached rows to read limit set by user')
#            print('Reached limit for first 24 hours and exiting last entry should have PickUp time of 01-06-2018  23:43:00')
            break
        try:
            row_tpep_pickup_datetime = row[1]
        except:
            print(f'Skipped row as Error during PU date assignment for row={rowsReadcount}')
            continue
#        print(f'row_tpep_pickup_datetime={row_tpep_pickup_datetime}')
        if (row_tpep_pickup_datetime < boundUpperPUDatetime and row_tpep_pickup_datetime >= boundLowerPUDatetime):
            joinedRow = ','.join(row)
            producer.send(topicName, joinedRow.encode('utf-8'))
            countSent2Producer = countSent2Producer + 1
            if countSent2Producer % 500000 == 0:
                print(f'Sent {rowsReadcount} row as the {countSent2Producer} th message to Kafka')
        elif not (row[0] == 'VendorID'):
#            print(f'Row {rowsReadcount} not sent to producer, row PUDatetime={row_tpep_pickup_datetime} and outside bounds')
            boundNotMatchRowsCount = boundNotMatchRowsCount + 1
# =============================================================================
#         if rowsReadcount % 50000 == 0:
#             print(f'Row={rowsReadcount}, Contents are:::\n{joinedRow}' )
#             print(f'Processed the {rowsReadcount} th row of csv and sent to Kafka' )
# =============================================================================

print("\n\nProcessed %d rows from csv\nSent %d messages to Kafka\nNOT SENT %d rows as OB" %(rowsReadcount-1, countSent2Producer, boundNotMatchRowsCount))
print('\nEndTime is:',datetime.now().strftime("%c"))