'''
Pre-requisite
1. Create environment variable STREAM_USERNAME and STREAM_PASSWORD with the stream username and password so that it is not exposed in code
This code performs the functionality of 2 layers in the architecture diagram.
1. In the 1st layer, which is data producer, it generates the random trade data and writes it into Trade_Data_List_YYYY_MM_DD_HH:MM:SSffffff.csv file and uploads it into the bronze(ratd_bronze_bucket) bucket.
2. In the 2nd layer, which is ingest/load, the randomly generated trade data is pushed into a Trade_stream by using OCI Streaming.
'''

import pandas as pd
from confluent_kafka import Producer
from datetime import datetime
import random
import oci
import time
import certifi
import os

# Store trade stream data in topic variable.
topic = "Trade_stream"  
message_list = []

# Configuration settings for connecting the Streaming servers and topics to produce the messages.
conf = {  
    'bootstrap.servers': "cell-1.streaming.us-phoenix-1.oci.oraclecloud.com:9092", 
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': certifi.where(),
    'sasl.mechanism': 'PLAIN',  
    # User Credentials 
    'sasl.username': os.environ.get('STREAM_USERNAME'), 
    'sasl.password': os.environ.get('STREAM_PASSWORD') 
   }  

# Function to create a list of trades in csv file
def create_trade_data_csv(no_of_rows):
    print("\t Start - in create_trade_data_csv method")   
    global message_list
    trade_tbl = []
    fund_list = ["SBI", "DSP","ICICI","PGIM","NIPPON","UTI","KOTAK","REL","HDFC","CANR"]
    sec_dict = {
                'Cipla': 116,
                'TCS': 315,
                'AxisBk': 91,
                'Eicher': 371,
                'Infosys': 152,
                'Airtel' : 81,
                'ITC' : 34,
                'HCL' : 103,
                'GAIL' : 646,
                'ONGC' : 13
                }
    trd_date_list = ["22/11/2022", "23/11/2022"]
    txn_type_list = ["BUY", "SELL"]
    qty_list = [25,50,75,100,125,150,175,200,225,250,275,300,325,350,375,400,425,450,475,500]
    trader_list = ["trader1", "trader2", "trader3"]
    broker_list = ["broker1", "broker2", "broker3"]

    # Write Randomly generated trade data into trade list.
    for i in range(0, no_of_rows):
        fund_id = random.choice(fund_list)
        sec_id, value = random.choice(list(sec_dict.items()))     
        trade_dt =  random.choice(trd_date_list)
        txn_type = random.choice(txn_type_list)
        qty = random.choice(qty_list)
        price = round(random.uniform(value-value/200,value+value/200), 2)
        trader = random.choice(trader_list)
        broker = random.choice(broker_list)
        trade_lst = [fund_id, sec_id, trade_dt, txn_type, qty, price, trader, broker]
        trade_tbl.append(trade_lst)
    
    # Create Wash Trade rows for POC (Negative test cases)    
    if include_neg_cases == True:
        trade_lst = ["CMXX", 'AMZN', '22/11/2022', 'SELL', 220, 2500, 'trader1', 'broker1']
        trade_tbl.append(trade_lst)
        trade_lst = ["CMXX", 'AMZN', '22/11/2022', 'BUY', 20, 2520, 'trader2', 'broker2']
        trade_tbl.append(trade_lst)
        trade_lst = ["CMXX", 'AMZN', '22/11/2022', 'BUY', 200, 2520.91, 'trader2', 'broker2']
        trade_tbl.append(trade_lst)
        trade_lst = ["CMXX", 'AAPL', '22/11/2022', 'SELL', 100, 325, 'trader1', 'broker1']
        trade_tbl.append(trade_lst)
        trade_lst = ["CMXX", 'AAPL', '22/11/2022', 'BUY', 100, 325.7, 'trader1', 'broker1']
        trade_tbl.append(trade_lst)

    # Create dataframe with trade list
    trade_df = pd.DataFrame(trade_tbl, columns=["FUND","SEC","TRADE_DT","TXN_TYPE","QTY","PRICE","TRADER","BROKER"])
    trade_df.to_csv('Trade_Data_List.csv',header =True, index=False)
    
    # Convert dataframe to dictionary 
    message_list = trade_df.to_dict(orient ='records')
    
    # Acknowledge that csv file is generated sucessfully
    print("\t\t Trade_Data_List.csv created Successfully")
    print("\t End - in create_trade_list method")
    

# Function to upload the Trade_Data_List.csv file to bronze bucket
def upload_file():
    print("\t Start - in upload_file method") 
    
    # Configuration settings for specifying the source and destination of the CSV file.
    object_storage = oci.object_storage.ObjectStorageClient(ociconfig)
    namespace = "apaciaas"
    bucket_name = "ratd_bronze_bucket" 
    object_name = "Trade_Data_Files/Trade_Data_List_" + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S%f') + ".csv" # generate file with timestamp
    fp = open("Trade_Data_List.csv",'rb')
    obj = object_storage.put_object(
        namespace,
        bucket_name,
        object_name,
        fp)

    # Acknowledge the File uploaded sucessfully to the Bronze bucket
    print("\t\t " + object_name + " uploaded Successfully")
    print("\t End - in upload_file method")

# Set variable delivered records to 0
delivered_records = 0 

# Function to acknowledge the records are produced sucessfully.  
def acked(err, msg):  
    global delivered_records 
    """Delivery report handler called on  
        successful or failed delivery of message """    
    if err is not None:  
        print("Failed to deliver message: {}".format(err))  
    else:  
        delivered_records += 1  
        print("Produced record to topic {} partition [{}] @ offset {}".format(msg.topic(), msg.partition(), msg.offset()))  

# Function to produce messages to Trade_stream
def produce_messages():

    # Create a producer instance to produce messages into the stream
    producer = Producer(**conf)  

    # Start producing the messages
    print("\t Start - in produce_messages method") 
    message = None
    i=0
    for message in message_list:  
        record_key = message["FUND"] + message["SEC"] + message["TRADE_DT"] + message["TXN_TYPE"]
        record_value = str(message)
        print("Producing record: {}\t{}".format(record_key, record_value))  
        i = i+1
        if i%100 == 0: 
            time.sleep(1)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)  
        producer.poll(0)    
        
    print("\t End - in produce_messages method") 
    producer.flush()  
    print("{} messages were produced to topic {}!".format(delivered_records, topic))


# Main function that calls all the sub functions
if __name__ == '__main__':
    ociconfig = oci.config.from_file()    
    runs = 1 
    rows_per_file = 5000 
    include_neg_cases = True 
    for i in range(0, runs):
        if i%10 == 0: 
            time.sleep(1)
        create_trade_data_csv(rows_per_file,include_neg_cases)
        upload_file()
        produce_messages()
