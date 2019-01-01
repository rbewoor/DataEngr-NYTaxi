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
