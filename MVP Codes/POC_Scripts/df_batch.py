import pandas as pd
import oci 

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from io import StringIO
import csv, re

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.application import MIMEApplication

#df_trades = spark.read.format('csv').option("header","true").load(output_location_1)
#display(df_trades)

#final_csv = open('export (3).csv','w')
pem_prefix = '-----BEGIN PRIVATE KEY-----\n'
pem_suffix = '\n-----END PRIVATE KEY-----'
key = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCweVl9tnJavSioO1UliKtl8qf69J52EJrDHITNbkDi4PD9IIOnntBiSmjxqXlfbT4XICRTbT4reuIaTWQQFkLh7AX1d+MPYEKbjgJUmsS0XOcFZImv2l07jKQGpnNg8ybH59D9/1v3Lpe4tP7kkqTCqpUPApuEtheUuLornTN+l80HHkhlAxQKmwhs6fzUumK6hK4QLejarUDeVUrBjroB6CGPXDMNfvb0nA5ih/50InWzV3eVeyH1x0erajL7W7pEnHshiLqTGYWbxC08EWtgzSqwsWinmx+rY/Fo06MWuHwwxmb3z8Ak4HKqVPTQkoZYzmWj48oXan03BY+AiHavAgMBAAECggEARrYZGbpFT/6DkAVWNNfydcMpc/EYnY5BtPR0ciw/a6leZs7kcgG81eWi71JNA+OuAW4roBIh2yI9/vQLqDaDTitYp+cF4F9d0R6x6FyrfOnM1+hVE1WYDghooRGJIcvMOkW1BFGR9BWDTcuYZtYrlqTrXTxaPG8KO9lZH6i5vXtBs/T18VfYz5ekGEAAg2CUvqriFGnlLZCJ1LhwYTqRdMuwf6XXCs52+miZHJn7AMYji14YzTw0sSmPdm0enFIAVbFwynusjsTRHf3NL6UhcLLA09FTvvT6e0KAITEP/VAeYKH9SQ2QW5U5m7UF0mDXPEya4SlX1tUMHcCeVrAeIQKBgQD3DUnR0oXCiAkGJhzOwhxK6bw74gqz6E1gcIfxjd/o+uA3ANYEQmwj47eoaNx0sbdQMHdI2p9KvcdUhJ6rdQPfjShifKh9Xaoka7cuqKTVMKvzTxs/lEy3ccL05BeVnYMjD3jdJw7k0XQ7J+tBFJgE4VeRFooNBYZIx2d6tbVblQKBgQC23aZaW0tsWAXTe+lJpePGeuGhGQCuV6p6t/Kn1n7xohn/RPUYrU5QHQYOL8O3+vMeKmF8ucvLi8cC8cus3h09Ll6z9eb+mTWDPd2Lwu+jHczlHcr2pwzN18gaoBjjLhqF2YKXZRmsP4UfcMz3tTTdcafQypvnGrwM8jQXwzxYMwKBgQC60QufZQjM/72DLtLd7p8ibvludxIM1X+di7rhCJ3nOb7PGQy9j9TiltJMwW7jt3edZejt6JRIGpZe7SJnGUdihwWg5A8tLeT5QZL174UlyXZduNYsD+KrXZVFRi4nb0K5AnwtD9oNYe34xcj6H66NEjH7fwXJrwHKiy9O9ZU8uQKBgD/IYfzEOTOKJEYWw1Ev7pnNRKPXP7iP1WPGg3ntRAvuGZlDKSY5VMZ2ySTrnh2vB1uvNp+1gpL1py2svvkF5Dbx1JB6pd6J+/NSAdN84+8GNvB3itKrg7jMmfxHeUbMTu3+5yD9X44H/dvwkV2ZM95FhV47PVPHrG3rkSX0sDinAoGASQprbY27kuDHax96JuZIi+zvNeXqkdDwJ4QGAI0JZT/a/KULQAhk8HSjSHoBtqFSWtMmC0OnqCe6Y3husn1EBG6nkA001PBZYpeApYyTSe+/aZGknZonOgxjPy96tiEAwigfA1GyFTii60iHY9HigYyomheURNq0oF71BZ91GHc="
    # The content of your private key
key_content = '{}{}{}'.format(pem_prefix, key, pem_suffix)

compartment_id = 'ocid1.compartment.oc1..aaaaaaaaim5pavnzdrxm2nylvxgpuj4o6m3lfe3vjua2h3fatae6a3kdhwfq'

config_with_key_content = {
    "user": 'ocid1.user.oc1..aaaaaaaaeks5zr4ln34774kkr36cw4ci7cxqtsc6yext6dr27vrqcmeqbqrq',
    "key_content": key_content,
    "fingerprint": 'fc:ac:72:57:0b:d8:05:70:5e:db:b5:5d:f4:3a:a5:19',
    "tenancy": 'ocid1.tenancy.oc1..aaaaaaaajt4yyodu3xeqezacrjhxddbkg2bbnx4xhvtbdbsktd66ycrmcntq',
    "region": 'us-phoenix-1'
}


config = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(config)
#function to get object from source bucket
list_objects_response = object_storage_client.list_objects(
    namespace_name="apaciaas",
    bucket_name="ratd_silver_bucket",
    prefix = "Trade_Files/part")

#print(list_objects_response.data)

for o in list_objects_response.data.objects:    
    print(o.name)
print("Successfully retrieved object from soruce bucket")
"""
spark_session = SparkSession.builder.appName("Trade_Detection_df").getOrCreate()

#df_trades= pd.read_csv(r"C:\Venus\OCI_MVP\RATD_MVP\Data_Flow\Trade_processing_Data.csv",header=0,dtype = {'START_TIME': str, 'END_TIME': str})
#df_trades =spark_session.read.format("csv").option("header","true").load(r"C:\Venus\OCI_MVP\RATD_MVP\Data_Flow\Trade_processing_Data.csv")
df_trades =spark_session.read.format("csv").option("header","true").load(r"C:\Venus\OCI_MVP\RATD_MVP\Data_Flow\Trade_processing_Data.csv")


#tradedata_agg = sql_context.sql("select key, START_TIME, END_TIME, sum('Market_Value') as Total_MV from tradeData group by key, START_TIME, END_TIME")

tradedata_agg = df_trades.groupBy("key","START_TIME","END_TIME").agg(sum("Market_Value").alias("Total_MV")).select(col('key').alias('akey'), col('START_TIME').alias('ST'), col('END_TIME').alias('ET'),col('Total_MV').alias('Total_MV'))

#tradedata_agg.display()

#final_tradedata = df_trades.join(tradedata_agg,df_trades.key == tradedata_agg.key, "inner")
final_tradedata = df_trades.join(tradedata_agg, (df_trades.key == tradedata_agg.akey) & (df_trades.START_TIME == tradedata_agg.ST) & (df_trades.END_TIME ==  tradedata_agg.ET),"inner")

#final_tradedata.display()

washtrades = final_tradedata.alias('l').join(final_tradedata.alias('r'), on='joinKey').where('l.FUND = r.FUND and l.SEC = r.SEC and l.TRADE_DT = r.TRADE_DT and l.START_TIME = r.START_TIME and l.END_TIME = r.END_TIME and (l.timestamp between l.START_TIME and r.END_TIME) and l.TXN_TYPE != r.TXN_TYPE and abs((l.Total_MV - r.Total_MV)/l.Total_MV )*100 < 1').select(col("l.FUND"),col("l.SEC"), col("l.TRADE_DT"),col("l.TXN_TYPE"),col("l.QTY"),col("l.PRICE"),col("l.TRADER"),col("l.BROKER"),col("l.timestamp").alias("Entry_Time"))
#final_tradedata.createOrReplaceTempView("TradeSummaryData")

#washtrades = spark.sql("SELECT l.FUND,l.SEC,l.TRADE_DT,l.TXN_TYPE,l.QTY,l.PRICE,l.TRADER,l.BROKER,abs(((r.Total_MV - l.Total_MV)/r.Total_MV ))*100 as MV_Tolerance  FROM TradeSummaryData l INNER JOIN TradeSummaryData r ON (l.START_TIME = r.START_TIME and l.END_TIME = r.END_TIME  and r.FUND = l.FUND and l.SEC = r.SEC and r.TRADE_DT = l.TRADE_DT) where r.TXN_TYPE != l.TXN_TYPE and abs(((r.Total_MV - l.Total_MV)/r.Total_MV ))*100 < 1") 

#washtrades.write.mode("overwrite").format("csv").option("header","true").save("C:\Venus\OCI_MVP\RATD_MVP\Data_Flow\Wash_Trades")


# MAIL sending configurations
fromaddr = "thunderbolt677@outlook.com"
toaddr = "gvenuslazarus@gmail.com"
# instance of MIMEMultipart
msg = MIMEMultipart()
# storing the senders email address
msg['From'] = fromaddr
# storing the receivers email address
msg['To'] = toaddr
# storing the subject
msg['Subject'] = "Wash trades information"
# string to store the body of the mail
body = "This information is regarding wash trades"
# attach the body with the msg instance
msg.attach(MIMEText(body, 'plain'))
# Name of the file to be sent

# File to be sent

part = MIMEApplication(washtrades.toPandas().to_csv(header=True,index=False), Name="WashTrades.csv")
part['Content-Disposition'] = 'attachment; filename="%s"' % 'WashTrades.csv'

#encoders.encode_base64(p)
#p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
# attach the instance 'p' to instance 'msg'
msg.attach(part)
## creates SMTP session
s = smtplib.SMTP('smtp-mail.outlook.com', 587)
# start TLS for security
s.starttls()
# Authentication
s.login(fromaddr, "Macnam007000")
# Converts the Multipart msg into a string
text = msg.as_string()
# sending the mail
s.sendmail(fromaddr, toaddr, text)
print("Successfully sent mail with the attached file")
# terminating the session
s.quit()
print("Email sent")

"""