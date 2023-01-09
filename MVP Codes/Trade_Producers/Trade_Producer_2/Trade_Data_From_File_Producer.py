'''
This code performs the functionality of pulishing random trade data from a Trade_Data.csv file to the stream. 
This code also uploads the CSV file to the Bronze Bucket(ratd_bronze_bucket) to be used for future analysis.
'''
import oci  
from base64 import b64encode
import csv
from datetime import datetime
  
# Configuration settings for connecting the Streaming servers and topics to produce the messages.
ociMessageEndpoint = "https://cell-1.streaming.us-phoenix-1.oci.oraclecloud.com"  
ociStreamOcid = "ocid1.stream.oc1.phx.amaaaaaapwxjxiqaj3lnlfg34dt74fjfoxlipfdiouv4mz75hntatthwbekq"    

# Relative path of the Trade_Data.csv file
csvfile = open('Trade_Data.csv', 'r')
fieldnames = ("FUND","SEC","TRADE_DT","TXN_TYPE","QTY","PRICE","TRADER","BROKER")
        
# Function to produce random trade data to Stream
def produce_messages(client, stream_id):
  
  # Reading data from .csv file and grouping them as key and value pair.
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

        # Key is an identifier which is used to group related messages and value to strore data.
        key =  key 
        value = str(row) 

        # Code to encode key and value and append it to message_list.
        encoded_key = b64encode(key.encode()).decode()
        encoded_value = b64encode(value.encode()).decode()
        message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))
  
  # Code to publish encoded messages to the stream.
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

# Function to upload .csv file containing trade data to the Bronze bucket
def upload_file():
    print("\t Start - in upload_file method")    
    object_storage = oci.object_storage.ObjectStorageClient(config)
    namespace = "apaciaas"
    bucket_name = "ratd_bronze_bucket"
    object_name =  "Trade_Data_Files/Trade_Data_List_" + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S%f') + ".csv" 
   
    # Give relative path of the Trade_Data.csv file
    fp = open('Trade_Data.csv','rb')
    obj = object_storage.put_object(
        namespace,
        bucket_name,
        object_name,
        fp)
    print("\t\t " + object_name + " uploaded Successfully")
    print("\t End - in upload_file method")

config = oci.config.from_file()
print("Post config")
stream_client = oci.streaming.StreamClient(config, service_endpoint=ociMessageEndpoint)
print("Post stream client")

# Function call to Publish messages to the stream
produce_messages(stream_client, ociStreamOcid)

#Upload the csv file to bronze bucket
upload_file()