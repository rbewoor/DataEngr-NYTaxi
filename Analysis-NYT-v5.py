# -*- coding: utf-8 -*-
"""
Created on Thu Jan  3 12:37:02 2019

@author: RB
"""
"""
ARRAY will be used to hold the aggregated values for each hour
arrAggregateHourlyValues
24 rows for each timeSlice of the overall period being analysed. Here 1 hour slices.
col 01: total trips - count
col 02: total passengers - summation
col 03: average passengers / trip - calculated value
col 04: store and forward flag=Y - count
col 05: store and forward flag=N - count
col 06: payment type - credit card (1) - count
col 07: payment type - cash        (2) - count
col 08: payment type - no charge   (3) - count
col 09: payment type - dispute     (4) - count
col 10: payment type - unknown     (5) - count
col 11: payment type - void        (6) - count
col 12: total fare amount - summation
col 13: average fare / trip - calculated value
col 14: total of trip distances - summations
col 15: average distance / trip - calculated value

LIST to hold aggregated hourly info: each of these elements repeats for each number of time slices being considered
   which is timeSliceNumber1 to timeSliceNumber24. Each timeSlice List has 6 items. Of these 6 items, the last three are
   lists themselves for PU, DO and PU-DO combo info. See struture below--
[
  ["timeSlice1","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice2","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  similarly till timeSlice24,
]
"""
from pymongo import MongoClient
from datetime import datetime
import numpy as np


print('\nStartTime is:',datetime.now().strftime("%c"))

client = MongoClient('localhost:27017')
dbName = client.TestNYTFullJuneButOnly24hrsDb1        # alternative way to code is:::  dbName = client['TwitterDb1']
collection = dbName.TestNYTFullJuneButOnly24hrsCol1   # alternative way to code is:::  collection = dbName['TwitterCol1']

# verified that accessing any one element is a string e.g. timeSlotList[4] is a string
timeSlotList=["2018-06-01 00:00:00","2018-06-01 01:00:00","2018-06-01 02:00:00","2018-06-01 03:00:00",
              "2018-06-01 04:00:00","2018-06-01 05:00:00","2018-06-01 06:00:00","2018-06-01 07:00:00",
              "2018-06-01 08:00:00","2018-06-01 09:00:00","2018-06-01 10:00:00","2018-06-01 11:00:00",
              "2018-06-01 12:00:00","2018-06-01 13:00:00","2018-06-01 14:00:00","2018-06-01 15:00:00",
              "2018-06-01 16:00:00","2018-06-01 17:00:00","2018-06-01 18:00:00","2018-06-01 19:00:00",
              "2018-06-01 20:00:00","2018-06-01 21:00:00","2018-06-01 22:00:00","2018-06-01 23:00:00",
              "2018-06-02 00:00:00"]

# =============================================================================
# Build the array arrAggregateHourlyValues[][] and populate it
# =============================================================================
arrAggregateHourlyValues = np.zeros((24,15))
for idx1 in range(0,24):
    
    timeBoundLower = timeSlotList[idx1]
    timeBoundUpper = timeSlotList[idx1 + 1]
    
    #get total number of trips
    pipeline = [ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$group": {"_id": 1, "TotalTripsCount": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalTripsCount = collection.aggregate(pipeline)
    for cursor in cursorTotalTripsCount:
        arrAggregateHourlyValues[idx1,0] = cursor["TotalTripsCount"]
    cursorTotalTripsCount.close()
    
    #get total number of passengers
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$group": {"_id": 1, "TotalPassengers": {"$sum": "$passenger_count"}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPassengers = collection.aggregate(pipeline)
    for cursor in cursorTotalPassengers:
        arrAggregateHourlyValues[idx1,1] = cursor["TotalPassengers"]
    cursorTotalPassengers.close()
    
    #get average number of passengers / trip
    arrAggregateHourlyValues[idx1,2] = arrAggregateHourlyValues[idx1,1] / arrAggregateHourlyValues[idx1,0]
    
    # total documents with Store and Forward Flag as YES
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"store_and_fwd_flag": "Y"}} , {"$group": {"_id": 1, "CountOfStoreFwdYES": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalStoreForwardYES = collection.aggregate(pipeline)
    for cursor in cursorTotalStoreForwardYES:
        arrAggregateHourlyValues[idx1,3] = cursor["CountOfStoreFwdYES"]
    cursorTotalStoreForwardYES.close()
    
    # total documents with Store and Forward Flag as NO
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"store_and_fwd_flag": "N"}} , {"$group": {"_id": 1, "CountOfStoreFwdNO": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalStoreForwardNO = collection.aggregate(pipeline)
    for cursor in cursorTotalStoreForwardNO:
        arrAggregateHourlyValues[idx1,4] = cursor["CountOfStoreFwdNO"]
    cursorTotalStoreForwardNO.close()
    
    # payment type - credit card (1)
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"payment_type": 1}} , {"$group": {"_id": 1, "CountPaidByCreditCard": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPaymentCreditCard = collection.aggregate(pipeline)
    for cursor in cursorTotalPaymentCreditCard:
        arrAggregateHourlyValues[idx1,5] = cursor["CountPaidByCreditCard"]
    cursorTotalPaymentCreditCard.close()
    
    # payment type - cash        (2)
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"payment_type": 2}} , {"$group": {"_id": 1, "CountPaidByCash": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPaymentCash = collection.aggregate(pipeline)
    for cursor in cursorTotalPaymentCash:
        arrAggregateHourlyValues[idx1,6] = cursor["CountPaidByCash"]
    cursorTotalPaymentCash.close()
    
    # payment type - no charge   (3)
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"payment_type": 3}} , {"$group": {"_id": 1, "CountPaidByNoCharge": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPaymentNoCharge = collection.aggregate(pipeline)
    for cursor in cursorTotalPaymentNoCharge:
        arrAggregateHourlyValues[idx1,7] = cursor["CountPaidByNoCharge"]
    cursorTotalPaymentNoCharge.close()
    
    # payment type - dispute     (4)
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"payment_type": 4}} , {"$group": {"_id": 1, "CountPaidByDispute": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPaymentDispute = collection.aggregate(pipeline)
    for cursor in cursorTotalPaymentDispute:
        arrAggregateHourlyValues[idx1,8] = cursor["CountPaidByDispute"]
    cursorTotalPaymentDispute.close()
    
    # payment type - unknown     (5)
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"payment_type": 5}} , {"$group": {"_id": 1, "CountPaidByUnknown": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPaymentUnknown = collection.aggregate(pipeline)
    for cursor in cursorTotalPaymentUnknown:
        arrAggregateHourlyValues[idx1,9] = cursor["CountPaidByUnknown"]
    cursorTotalPaymentUnknown.close()
    
    # payment type - void        (6)
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$match": {"payment_type": 6}} , {"$group": {"_id": 1, "CountPaidByVoid": {"$sum": 1}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalPaymentVoid = collection.aggregate(pipeline)
    for cursor in cursorTotalPaymentVoid:
        arrAggregateHourlyValues[idx1,10] = cursor["CountPaidByVoid"]
    cursorTotalPaymentVoid.close()
    
    #get total fare paid
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$group": {"_id": 1, "TotalOfFare": {"$sum": "$total_amount"}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalFare = collection.aggregate(pipeline)
    for cursor in cursorTotalFare:
        arrAggregateHourlyValues[idx1,11] = cursor["TotalOfFare"]
    cursorTotalFare.close()
    
    #get average fare / trip
    arrAggregateHourlyValues[idx1,12] = arrAggregateHourlyValues[idx1,11] / arrAggregateHourlyValues[idx1,0]
    
    #get total trip distances
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$group": {"_id": 1, "TotalDistance": {"$sum": "$trip_distance"}}} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTotalDistance = collection.aggregate(pipeline)
    for cursor in cursorTotalDistance:
        arrAggregateHourlyValues[idx1,13] = cursor["TotalDistance"]
    cursorTotalDistance.close()
    
    #get average distance / trip
    arrAggregateHourlyValues[idx1,14] = arrAggregateHourlyValues[idx1,13] / arrAggregateHourlyValues[idx1,0]
    
# =============================================================================
# Print the arrAggregateHourlyValues array as it is
# =============================================================================
#print(arrAggregateHourlyValues)

# =============================================================================
# Print the arrAggregateHourlyValues[][] to console in readable way
# =============================================================================
print(f'\n\nAnalysis data:')
for colIdx in range(0,15):
    print('\n')
    if colIdx == 0:
        print(f'Total Trips per hour:')
    elif colIdx == 1:
        print(f'Total Passenger per hour:')
    elif colIdx == 2:
        print(f'Average Passerger/Trip per hour:')
    elif colIdx == 3:
        print(f'Count of StoreFwd=YES per hour:')
    elif colIdx == 4:
        print(f'Count of StoreFwd=NO per hour:')
    elif colIdx == 5:
        print(f'Count Payment CREDIT CARD per hour:')
    elif colIdx == 6:
        print(f'Count Payment CASH per hour:')
    elif colIdx == 7:
        print(f'Count Payment NO CHARGE per hour:')
    elif colIdx == 8:
        print(f'Count Payment DISPUTE per hour:')
    elif colIdx == 9:
        print(f'Count Payment UNKNOWN per hour:')
    elif colIdx == 10:
        print(f'Count Payment VOID per hour:')
    elif colIdx == 11:
        print(f'Total Fare Amount per hour:')
    elif colIdx == 12:
        print(f'Average Fare/Trip per hour:')
    elif colIdx == 13:
        print(f'Total Distance per hour:')
    elif colIdx == 14:
        print(f'Average Distance/Trip per hour:')
    for rowIdx in range(0,24):
        print(f'Hour%d = %.2f -- ' %(rowIdx, arrAggregateHourlyValues[rowIdx][colIdx]), end="", flush=True)


# =============================================================================
# # LIST BUILDING for PICK UP AND DROP OFF ANALYSIS
# =============================================================================
# =============================================================================
# List initialise start
# =============================================================================
listLocInfoAggregateHourlyValues =                                                             \
[
  ["timeSlice1","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice2","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice3","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice4","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice5","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice6","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice7","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                          
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice8","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                          
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice9","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                          
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice10","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice11","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice12","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice13","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice14","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice15","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice16","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice17","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice18","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice19","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice20","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice21","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,
  ["timeSlice22","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice23","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ] ,                                                                                                        
  ["timeSlice24","timeSliceStart-UNFILLED","timeSliceEnd-UNFILLED" ,                                         
	[ ['TopPULoc1','PULoc1-UNFILLED',0], ['TopPULoc2','PULoc2-UNFILLED',0], ['TopPULoc3','PULoc3-UNFILLED',0], ['TopPULoc4','PULoc4-UNFILLED',0], ['TopPULoc5','PULoc5-UNFILLED',0] ] ,
	[ ['TopDOLoc1','DOLoc1-UNFILLED',0], ['TopDOLoc2','DOLoc1-UNFILLED',0], ['TopDOLoc3','DOLoc3-UNFILLED',0], ['TopDOLoc4','DOLoc4-UNFILLED',0], ['TopDOLoc5','DOLoc5-UNFILLED',0] ] ,
	[ ['TopPUDOCombo1','PUDOLoc1-UNFILLED',0], ['TopPUDOCombo2','PUDOLoc2-UNFILLED',0], ['TopPUDOCombo3','PUDOLoc3-UNFILLED',0], ['TopPUDOCombo4','PUDOLoc4-UNFILLED',0], ['TopPUDOCombo5','PUDOLoc5-UNFILLED',0] ]
  ]
]
# =============================================================================
# --- x[23][3][0][1] = 'PULoc1-UNFILLED'
# --- x[23][3][0] = ['TopPULoc1', 'PULoc1-UNFILLED', 0]
# --- x[23][3] = [['TopPULoc1', 'PULoc1-UNFILLED', 0], ['TopPULoc2', 'PULoc2-UNFILLED', 0], ['TopPULoc3', 'PULoc3-UNFILLED', 0]]
# --- x[23][1] = 'timeSliceStart-UNFILLED'
# =============================================================================
# =============================================================================
# LIST Initialisation end
# =============================================================================


# =============================================================================
# print('\n\nThe location list INITIAL VALUES::')
# for idx in range(0,24):
#     print('\nTime slice number %d' %(idx + 1))
#     print(listLocInfoAggregateHourlyValues[idx][0], ' -- ' , listLocInfoAggregateHourlyValues[idx][1], ' -- ' , listLocInfoAggregateHourlyValues[idx][2])
#     print(listLocInfoAggregateHourlyValues[idx][3])
#     print(listLocInfoAggregateHourlyValues[idx][4])
#     print(listLocInfoAggregateHourlyValues[idx][5])
# =============================================================================


#print(listLocInfoAggregateHourlyValues)
# =============================================================================
# LIST POPULATION start
# =============================================================================
for idxTimeSlice in range(0,24):
    
    timeBoundLower = timeSlotList[idxTimeSlice]
    timeBoundUpper = timeSlotList[idxTimeSlice + 1]
    listLocInfoAggregateHourlyValues[idxTimeSlice][1] = timeBoundLower
    listLocInfoAggregateHourlyValues[idxTimeSlice][2] = timeBoundUpper
    # find top 5 Pick Up locations by count
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 00:00:00"}}} , {"$group":{"_id": "$PULocationID", "countPULocTrips": {"$sum": 1}}} , {"$sort": {"countPULocTrips": -1}}, {"$limit": 5} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTop3PULocsByCount = collection.aggregate(pipeline)
    idx1 = 0
    for cursor in cursorTop3PULocsByCount:
        listLocInfoAggregateHourlyValues[idxTimeSlice][3][idx1][1] = cursor["_id"]
        listLocInfoAggregateHourlyValues[idxTimeSlice][3][idx1][2] = cursor["countPULocTrips"]
        idx1 = idx1 + 1
    cursorTop3PULocsByCount.close()
    
    # find top 5 Drop Off locations by count
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-01 01:00:00"}}} , {"$group":{"_id": "$DOLocationID", "countDOLocTrips": {"$sum": 1}}} , {"$sort": {"countDOLocTrips": -1}}, {"$limit": 5} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTop3DOLocsByCount = collection.aggregate(pipeline)
    idx1 = 0
    for cursor in cursorTop3DOLocsByCount:
        listLocInfoAggregateHourlyValues[idxTimeSlice][4][idx1][1] = cursor["_id"]
        listLocInfoAggregateHourlyValues[idxTimeSlice][4][idx1][2] = cursor["countDOLocTrips"]
        idx1 = idx1 + 1
    cursorTop3DOLocsByCount.close()
    
    # find top 5 COMBINATION OF Pick Up TO Drop Off locations by count
    pipeline=[ {"$match": {"tpep_pickup_datetime": {"$gte": "2018-06-01 00:00:00", "$lt": "2018-06-02 00:00:00"}}} ,{"$project": { "PUDOCombo": { "$concat": [{"$substr":["$PULocationID", 0, -1]}, " TO " , {"$substr":["$DOLocationID", 0, -1]}]}}} ,{"$group":{"_id": "$PUDOCombo", "countPUtoDOLocTrips": {"$sum": 1}}} ,{"$sort": {"countPUtoDOLocTrips": -1}},{"$limit": 5} ]
    pipeline[0]['$match']['tpep_pickup_datetime']['$gte'] = timeBoundLower
    pipeline[0]['$match']['tpep_pickup_datetime']['$lt'] = timeBoundUpper
    cursorTop5ComboDOtoPULocsByCount = collection.aggregate(pipeline)
    idx1 = 0
    for cursor in cursorTop5ComboDOtoPULocsByCount:
        listLocInfoAggregateHourlyValues[idxTimeSlice][5][idx1][1] = cursor["_id"]
        listLocInfoAggregateHourlyValues[idxTimeSlice][5][idx1][2] = cursor["countPUtoDOLocTrips"]
        idx1 = idx1 + 1
    cursorTop5ComboDOtoPULocsByCount.close()
# =============================================================================
# LIST POPULATION end
# =============================================================================
print('\n\nThe location list AFTER populating from db::')
for idx in range(0,24):
    print('\nTime slice number %d' %(idx + 1))
    print(listLocInfoAggregateHourlyValues[idx][0], ' -- ' , listLocInfoAggregateHourlyValues[idx][1], ' -- ' , listLocInfoAggregateHourlyValues[idx][2])
    print(listLocInfoAggregateHourlyValues[idx][3])
    print(listLocInfoAggregateHourlyValues[idx][4])
    print(listLocInfoAggregateHourlyValues[idx][5])
#print(listLocInfoAggregateHourlyValues)

# =============================================================================
# VISUALISATION PROCESSING start
# =============================================================================



# =============================================================================
# VISUALISATION PROCESSING end
# =============================================================================

print('\nProgram Endtime is:',datetime.now().strftime("%c"))
#retDoc = colName.find_one()

#retDocUser = retDoc['user']

#print('\nDocument field created_at value is:', retDoc['created_at'])
#print('\nDocument field user.screen_name value is:', retDocUser['screen_name'])

#keys = retDoc.keys()

#print('\n\nFull document:',retDoc,'\n\n')

#keys_str = str(keys)


