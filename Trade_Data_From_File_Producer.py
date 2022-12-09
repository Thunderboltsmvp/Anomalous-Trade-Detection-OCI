from operator import index, indexOf
import random
from unicodedata import name
import oci  
from base64 import b64encode, b64decode 
import csv
from datetime import datetime
  
# OCI credentials
ociMessageEndpoint = "https://cell-1.streaming.us-phoenix-1.oci.oraclecloud.com"  
ociStreamOcid = "ocid1.stream.oc1.phx.amaaaaaapwxjxiqaj3lnlfg34dt74fjfoxlipfdiouv4mz75hntatthwbekq"    
csvfile = open('C:\Venus\OCI_MVP\RATD_MVP\Trade_Data.csv', 'r')
fieldnames = ("FUND","SEC","TRADE_DT","TXN_TYPE","QTY","PRICE","TRADER","BROKER")

# Function to produce messages  
def produce_messages(client, stream_id):
 
  # Build up a PutMessagesDetails and publish some messages to the stream
  reader = csv.DictReader(csvfile)
  message_list = []
  header_list = ("FUND","SEC","TRADE_DT","TXN_TYPE")
  for eachrow in reader:
        row = {}
        key = ''
        for field in fieldnames:
            if field == "QTY" or field == "PRICE":
                row[field]= float(eachrow[field])
            else:
                row[field] = eachrow[field]
            if field in header_list:
                key = key + row[field]
        print(key)
        print(row)
        key =  key 
        value = str(row) 
        encoded_key = b64encode(key.encode()).decode()
        encoded_value = b64encode(value.encode()).decode()
        message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))
  
  print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
  messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
  print("Message {}", messages)
  put_message_result = client.put_messages(stream_id, messages)
  
  # The put_message_result can contain some useful metadata for handling failures
  for entry in put_message_result.data.entries:
      if entry.error:
          print("Error ({}) : {}".format(entry.error, entry.error_message))
      else:
          print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))

# Function to upload Trade_Data.csv file to bronze bucket
def upload_file():
    print("\t Start - in upload_file method")    
    object_storage = oci.object_storage.ObjectStorageClient(config)
    namespace = "apaciaas"
    bucket_name = "ratd_bronze_bucket"
    object_name =  "Trade_Data_Files/Trade_Data_List_" + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S%f') + ".csv" 
    fp = open('C:\Venus\OCI_MVP\RATD_MVP\Trade_Data.csv','rb')
    obj = object_storage.put_object(
        namespace,
        bucket_name,
        object_name,
        fp)
    print("\t\t " + object_name + " uploaded Successfully")
    print("\t End - in upload_file method")


config = oci.config.from_file()
print("post config")

stream_client = oci.streaming.StreamClient(config, service_endpoint=ociMessageEndpoint)
print("post stream client")

# Publish messages to the stream
produce_messages(stream_client, ociStreamOcid)
upload_file()