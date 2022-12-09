from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import concat, col, current_timestamp, lit, window, \
  substring, to_timestamp, explode, split, length, from_csv, from_json
import math
import string
import random 
import argparse
import os
from base64 import b64encode, b64decode 
import time
import json

# Main function 
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--auth-type', default='PLAIN')
  parser.add_argument('--bootstrap-port', default='9092')
  parser.add_argument('--bootstrap-server')
  parser.add_argument('--checkpoint-location')
  parser.add_argument('--encryption', default='SASL_SSL')
  parser.add_argument('--ocid') #, default = 'ocid1.streampool.oc1.phx.amaaaaaapwxjxiqalvkq5ofneitmmhjmcc4ck5wtentuckeazjsbci4rrz4a')
  parser.add_argument('--output-location')
  parser.add_argument('--output-mode', default='file')
  parser.add_argument('--stream-password')
  parser.add_argument('--raw-stream')
  parser.add_argument('--stream-username')
  args = parser.parse_args()
 
  # Display necessary Exceptions if occured.
  if args.bootstrap_server is None:
    args.bootstrap_server = os.environ.get('BOOTSTRAP_SERVER')
  if args.raw_stream is None:
    args.raw_stream = os.environ.get('RAW_STREAM')
  if args.stream_username is None:
    args.stream_username = os.environ.get('STREAM_USERNAME')
  if args.stream_password is None:
    args.stream_password = os.environ.get('STREAM_PASSWORD')
    
  assert args.bootstrap_server is not None, "Kafka bootstrap server (--bootstrap-server) name must be set"
  assert args.checkpoint_location is not None, "Checkpoint location (--checkpoint-location) must be set"
  assert args.output_location is not None, "Output location (--output-location) must be set"
  assert args.raw_stream is not None, "Kafka topic (--raw-stream) name must be set"
 
 # Initialize spark
  spark = (
    SparkSession.builder
      .appName('ratd_data_flow')
      .config('failOnDataLoss', 'true')
      .config('spark.sql.streaming.minBatchesToRetain', '10')
      .config('spark.sql.shuffle.partitions', '1')
      .config('spark.sql.streaming.stateStore.maintenanceInterval', '300')
      .getOrCreate()
  )

  # Uncomment following line to enable debug log level.
  spark.sparkContext.setLogLevel('DEBUG')
 
  # Configure Kafka connection settings.
  if args.ocid is not None:
    jaas_template = 'com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule required intent="streamPoolId:{ocid}";'
    args.auth_type = 'OCI-RSA-SHA256'
  else:
    jaas_template = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";'
 
 # Credentials of kafka stream
  raw_kafka_options = {
    'kafka.sasl.jaas.config': jaas_template.format(
      username=args.stream_username, password=args.stream_password,
      ocid=args.ocid
    ),
    'kafka.sasl.mechanism': args.auth_type,
    'kafka.security.protocol': args.encryption,
    'kafka.bootstrap.servers': '{}:{}'.format(args.bootstrap_server,
                                              args.bootstrap_port),
    'subscribe': args.raw_stream,
    'kafka.max.partition.fetch.bytes': 1024 * 1024,
    'startingOffsets': 'latest'
  }

  print('kafka.sasl.jaas.config : ' + jaas_template.format(username=args.stream_username, password=args.stream_password,ocid=args.ocid))
  print( 'kafka.sasl.mechanism : ' +args.auth_type)
  print( 'kafka.security.protocol : ' +args.encryption)
  print('kafka.bootstrap.servers : ' + '{}:{}'.format(args.bootstrap_server, args.bootstrap_port))
  print( 'subscribe : ' +args.raw_stream) 
  print(" before spark raw code")
  
  # Read the raw Kafka stream.
  raw = spark.readStream.format('kafka').options(**raw_kafka_options).load()
  tradedata_df1 = raw.selectExpr("CAST(value AS STRING)", "CAST(key AS STRING)", "timestamp")
  tradedata_schema_string = "FUND STRING, SEC STRING, TRADE_DT STRING, TXN_TYPE STRING, QTY DOUBLE, PRICE DOUBLE, TRADER STRING, BROKER STRING"
  tradedata_df2 = tradedata_df1.select(from_json(col("value"), tradedata_schema_string).alias("tradedata"), "timestamp", "key")
  tradedata_df3 = tradedata_df2.select("tradedata.*", "timestamp", "key")
  tradedata_df3 = tradedata_df3.withColumn("joinKey", concat(tradedata_df3.FUND,tradedata_df3.SEC,tradedata_df3.TRADE_DT))
  tradedata_df3 = tradedata_df3.withColumn("Market_Value", tradedata_df3.QTY * tradedata_df3.PRICE)

  # Filter wash trades in the 30 sec interval
  washTrades_df = (
      tradedata_df3
        .withWatermark('timestamp', '30 seconds')
        .groupBy('FUND','SEC','TRADE_DT','TXN_TYPE','QTY','PRICE','TRADER','BROKER', 'joinKey','Market_Value','key', 'timestamp',window('timestamp', '30 seconds'))
        .count()
        .selectExpr('CAST(window.start AS timestamp) AS START_TIME',
                    'CAST(window.end AS timestamp) AS END_TIME',
                    'timestamp AS Timestamp','joinKey AS joinKey','FUND AS FUND','SEC AS SEC','TRADE_DT AS TRADE_DT', 'TXN_TYPE AS TXN_TYPE','QTY AS QTY','PRICE AS PRICE','TRADER AS TRADER','BROKER AS BROKER','Market_Value AS Market_Value',
                    'key AS key').alias("aggtradedata")
    )
    
# Write Data to CSV file in Data lake
  print("Writing aggregates to Object Storage")
  query = (
      washTrades_df.writeStream
        .format('csv')
        .option("header","true")
        .outputMode('append')
        .trigger(processingTime='30 seconds')
        .option('checkpointLocation', args.checkpoint_location)
        .option('path', args.output_location)
        .start()
  )

  query.awaitTermination()
 
main()