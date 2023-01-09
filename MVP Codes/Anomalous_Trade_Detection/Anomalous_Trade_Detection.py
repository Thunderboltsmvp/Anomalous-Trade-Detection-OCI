'''
This is the code for data flow(Batch) in persist/tranform layer of architecture diagram.
This spark batch data flow application is triggered every 30 mins by data integration ingests the CSV file which was produced by
the streaming batch flow application earlier and performs aggregation based on the start time, end time, fund, security, trade date, and 
transaction type. Then gets the total market value for this group and checks if the total market value falls within 1% tolerance 
for the matching buy and sell trades, and then marks all the matching buy and sell transactions as 
wash trades. This wash trades data gets stored in our data lake’s gold bucket (ratd_gold_bucket)
inside a folder named Wash_Trades. This batch data flow application also sends an email notification with the wash trades information to a 
configured email-id, this email contains information about all the wash trades which are detected in form of a CSV file.
''' 

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from functools import reduce
import oci

# Parse the inputs arguments
parser = argparse.ArgumentParser()
parser.add_argument('--output_path_gold_lake', type=str, help='Provide golden lake output dir path', required=True )
parser.add_argument('--output_format', type=str, help='Provide output format', required=True )
parser.add_argument('--input_format', type=str, help='Provide input format', required=True )
parser.add_argument('--Files_For_Processing_Path', type=str, help='Provide silver lake processing file path', required=True)
parser.add_argument('--Files_After_Processing_Path', type=str, help='Provide silver lake backup processing file path', required=True)
parser.add_argument('--ratd_bucket', type=str, help='Provide silver bucket name', required=True)
parser.add_argument('--ratd_namespace', type=str, help='Provide the namespace', required=True)
parser.add_argument('--FromEmail', type=str, help='Provide From email address', required=True )
parser.add_argument('--ToEmail', type=str, help='Provide To email address', required=True )
parser.add_argument('--FromEmailPwd', type=str, help='Provide From email address Password', required=True )
args = parser.parse_args()

# Assign the input and the output paths
golden_lake_target_path = args.output_path_gold_lake
output_format = args.output_format
input_format = args.input_format
Files_For_Processing_Path = args.Files_For_Processing_Path
Files_After_Processing_Path = args.Files_After_Processing_Path
vbucket_name = args.ratd_bucket
vnamespace = args.ratd_namespace
FromEmail = args.FromEmail
ToEmail = args.ToEmail
FromEmailPwd = args.FromEmailPwd

# Config file
pem_prefix = '-----BEGIN PRIVATE KEY-----\n'
pem_suffix = '\n-----END PRIVATE KEY-----'

# PRIVATE KEY CONTENT
key = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCweVl9tnJavSioO1UliKtl8qf69J52EJrDHITNbkDi4PD9IIOnntBiSmjxqXlfbT4XICRTbT4reuIaTWQQFkLh7AX1d+MPYEKbjgJUmsS0XOcFZImv2l07jKQGpnNg8ybH59D9/1v3Lpe4tP7kkqTCqpUPApuEtheUuLornTN+l80HHkhlAxQKmwhs6fzUumK6hK4QLejarUDeVUrBjroB6CGPXDMNfvb0nA5ih/50InWzV3eVeyH1x0erajL7W7pEnHshiLqTGYWbxC08EWtgzSqwsWinmx+rY/Fo06MWuHwwxmb3z8Ak4HKqVPTQkoZYzmWj48oXan03BY+AiHavAgMBAAECggEARrYZGbpFT/6DkAVWNNfydcMpc/EYnY5BtPR0ciw/a6leZs7kcgG81eWi71JNA+OuAW4roBIh2yI9/vQLqDaDTitYp+cF4F9d0R6x6FyrfOnM1+hVE1WYDghooRGJIcvMOkW1BFGR9BWDTcuYZtYrlqTrXTxaPG8KO9lZH6i5vXtBs/T18VfYz5ekGEAAg2CUvqriFGnlLZCJ1LhwYTqRdMuwf6XXCs52+miZHJn7AMYji14YzTw0sSmPdm0enFIAVbFwynusjsTRHf3NL6UhcLLA09FTvvT6e0KAITEP/VAeYKH9SQ2QW5U5m7UF0mDXPEya4SlX1tUMHcCeVrAeIQKBgQD3DUnR0oXCiAkGJhzOwhxK6bw74gqz6E1gcIfxjd/o+uA3ANYEQmwj47eoaNx0sbdQMHdI2p9KvcdUhJ6rdQPfjShifKh9Xaoka7cuqKTVMKvzTxs/lEy3ccL05BeVnYMjD3jdJw7k0XQ7J+tBFJgE4VeRFooNBYZIx2d6tbVblQKBgQC23aZaW0tsWAXTe+lJpePGeuGhGQCuV6p6t/Kn1n7xohn/RPUYrU5QHQYOL8O3+vMeKmF8ucvLi8cC8cus3h09Ll6z9eb+mTWDPd2Lwu+jHczlHcr2pwzN18gaoBjjLhqF2YKXZRmsP4UfcMz3tTTdcafQypvnGrwM8jQXwzxYMwKBgQC60QufZQjM/72DLtLd7p8ibvludxIM1X+di7rhCJ3nOb7PGQy9j9TiltJMwW7jt3edZejt6JRIGpZe7SJnGUdihwWg5A8tLeT5QZL174UlyXZduNYsD+KrXZVFRi4nb0K5AnwtD9oNYe34xcj6H66NEjH7fwXJrwHKiy9O9ZU8uQKBgD/IYfzEOTOKJEYWw1Ev7pnNRKPXP7iP1WPGg3ntRAvuGZlDKSY5VMZ2ySTrnh2vB1uvNp+1gpL1py2svvkF5Dbx1JB6pd6J+/NSAdN84+8GNvB3itKrg7jMmfxHeUbMTu3+5yD9X44H/dvwkV2ZM95FhV47PVPHrG3rkSX0sDinAoGASQprbY27kuDHax96JuZIi+zvNeXqkdDwJ4QGAI0JZT/a/KULQAhk8HSjSHoBtqFSWtMmC0OnqCe6Y3husn1EBG6nkA001PBZYpeApYyTSe+/aZGknZonOgxjPy96tiEAwigfA1GyFTii60iHY9HigYyomheURNq0oF71BZ91GHc="

# The content of your private key in formatted form so that it can be used by OCI functions
key_content = '{}{}{}'.format(pem_prefix, key, pem_suffix)

# Config file with key content
config_file = {
    "user": 'ocid1.user.oc1..aaaaaaaaeks5zr4ln34774kkr36cw4ci7cxqtsc6yext6dr27vrqcmeqbqrq',
    "key_content": key_content,
    "fingerprint": 'fc:ac:72:57:0b:d8:05:70:5e:db:b5:5d:f4:3a:a5:19',
    "tenancy": 'ocid1.tenancy.oc1..aaaaaaaajt4yyodu3xeqezacrjhxddbkg2bbnx4xhvtbdbsktd66ycrmcntq',
    "region": 'us-phoenix-1'
}
object_storage_client = oci.object_storage.ObjectStorageClient(config_file)

def main():

# Function to get files from source bucket
 list_objects_response = object_storage_client.list_objects(
    namespace_name=vnamespace,
    bucket_name=vbucket_name,
    prefix = Files_For_Processing_Path +"/part")

 filelist = []
 mfilelist = []
 for o in list_objects_response.data.objects:
    print(o.name)
    filelist.append('oci://' + vbucket_name + "@" + vnamespace + '/' + o.name)
    mfilelist.append(o.name)
   
 # Create spark session and SQL context
 spark_session = SparkSession.builder.appName("Trade_Detection_df").getOrCreate()

 tradedata_list = []
 FileExists = False

 # Iterate through the filelist to load into dataframe
 for file in filelist:
    itradedata_df = spark_session.read.format(input_format).option("header","true").load(file)
    if itradedata_df.count() > 0:
        tradedata_list.append(itradedata_df)    
        print("Loaded data into tradedata_df : " + file)
        FileExists = True

 if FileExists == True:
    # Get all data in one dataframe 
    tradedata_df = reduce(DataFrame.unionByName, tradedata_list).dropDuplicates(subset = ['START_TIME','END_TIME','Timestamp','FUND','SEC','TRADE_DT','TXN_TYPE','QTY','PRICE','TRADER','BROKER','Market_Value','key']) 
    print("Created main trade data frame successfully")
    
    # Get Total market value based on the time window
    tradedata_agg = tradedata_df.groupBy("key","START_TIME","END_TIME").agg(sum("Market_Value").alias("Total_MV")).select(col('key').alias('akey'), col('START_TIME').alias('ST'), col('END_TIME').alias('ET'),col('Total_MV').alias('Total_MV'))
    print("Aggregated Total_MV based on FUND, SEC, TRADE_DT, TXN_TYPE and Time window")

    # Add the Total market value to the main dataframe
    final_tradedata = tradedata_df.join(tradedata_agg, (tradedata_df.key == tradedata_agg.akey) & (tradedata_df.START_TIME == tradedata_agg.ST) & (tradedata_df.END_TIME ==  tradedata_agg.ET),"inner")
    print("Added Total_MV to final_tradedata")

    # Get the final data into a table for querying
    final_tradedata.createOrReplaceTempView("TradeSummaryData")

    # Do self join on the final table to get the wash trades using spark sql
    washtrades = spark_session.sql("SELECT distinct l.FUND,l.SEC,l.TRADE_DT,l.TXN_TYPE,l.QTY,l.PRICE,l.TRADER,l.BROKER,l.timestamp as Entry_Time  FROM TradeSummaryData l INNER JOIN TradeSummaryData r ON (l.FUND = r.FUND and l.SEC = r.SEC and l.TRADE_DT = r.TRADE_DT and l.START_TIME = r.START_TIME and l.END_TIME = r.END_TIME and l.TXN_TYPE != r.TXN_TYPE and abs(((l.Total_MV - r.Total_MV)/l.Total_MV ))*100 < 1)") 
    print("Wash Trades processing done")

    # Write the file to Gold Bucket
    washtrades.write.mode("overwrite").format(output_format).option("header","true").save(golden_lake_target_path)
    print("Wash Trades written to gold lake")
    
    #html email template
    emailTemplate = """\
                <html>
                <head>
                <style>
                body {
                        background-color: #f6f6f6;
                        font-family: sans-serif;
                        -webkit-font-smoothing: antialiased;
                        font-size: 14px;
                        line-height: 1.4;
                        margin: 0;
                        padding: 0;
                        -ms-text-size-adjust: 100%;
                        -webkit-text-size-adjust: 100%; }
                table {
                        border-collapse: separate;
                        mso-table-lspace: 0pt;
                        mso-table-rspace: 0pt;
                        width: 100%; }
                        table td {
                        font-family: sans-serif;
                        font-size: 14px;
                        vertical-align: top; }
                .body {
                        background-color: #f6f6f6;
                        width: 100%; }
                .content {
                        box-sizing: border-box;
                        display: block;
                        margin: 0 auto;
                        max-width: 580px;
                        padding: 10px; }
                .main {
                        background: #ffffff;
                        border-radius: 3px;
                        width: 100%; }

                .wrapper {
                        box-sizing: border-box;
                        padding: 20px; }

                .content-block {
                        padding-bottom: 10px;
                        padding-top: 10px;
                    }
                </style>
                </head>
                <body>
                <table class="main" >
                <div class="content">
                <tr>
                <td class="wrapper"><b>Wash Trades Detected!</b><td>
                </tr>
                <tr>
                <td class="wrapper">Wash Trades have been detected as per the IRS Wash Sale Rule (IRC Section 1091).<br>Please find attached the Wash Trades file.<td>
                </tr>
                <tr>
                <td class="wrapper">Regards, <br>Anomalous Trade Detection MVP<td>
                </tr>
                <div>
                </table>
                <body>
                </html>
                """
    
    # Call the function to send the email
    send_email(washtrades,emailTemplate)

    # Move files to processed Silver bucket
    moveFiles(mfilelist)
 else:
    print("No Files to Process")


# Function to send email containing wash trades to a configured email ID 
def send_email(washtradesdf,emailTemplate):
    
    print("In send_email function")
    
    # Email sending configurations
    fromaddr = FromEmail
    toaddr = ToEmail

    # Instance of MIMEMultipart
    msg = MIMEMultipart()

    # Storing the senders email address
    msg['From'] = fromaddr

    # Storing the receivers email address
    msg['To'] = toaddr

    # Storing the subject
    msg['Subject'] = "Wash Trades Detected"

    # String to store the body of the mail
    #Uncomment and use body in msg.attach instead of emailTemplate if you want to use plain
    #body = "Wash Trades have been detected as per the IRS Wash Sale Rule (IRC Section 1091). Please find attached the Wash Trades detected."
    
    # Attach the body with the msg instance
    msg.attach(MIMEText(emailTemplate, 'html'))

    # File to be sent
    part = MIMEApplication(washtradesdf.toPandas().to_csv(header=True,index=False), Name="WashTrades.csv")
    part['Content-Disposition'] = 'attachment; filename="%s"' % 'WashTrades.csv'

    # Attach the instance 'p' to instance 'msg'
    msg.attach(part)

    # Create SMTP session
    s = smtplib.SMTP('smtp-mail.outlook.com', 587)
    
    # Start TLS for security
    s.starttls()
    
    # Authentication
    s.login(fromaddr, FromEmailPwd)
    
    # Convert the Multipart msg into a string
    text = msg.as_string()
    
    # Send the email
    s.sendmail(fromaddr, toaddr, text)
    print("Successfully sent mail with the attached file")
    
    # Terminate the session
    s.quit()
    print("Email sent")


# Function to move files from one folder to another in silver bucket after processing
def moveFiles(mfilelist):
 print("Moving files Started")
 for file in mfilelist:

    # Function to get object from Trade_Files_For_Processing folder
    get_object_response = object_storage_client.get_object(
        namespace_name=vnamespace,
        bucket_name=vbucket_name,
        object_name=file) 
    print("Successfully retrieved object " + file + " from Trade_Files_For_Processing folder" )

    # Function to put object into Trade_Files_After_Processing
    obj = object_storage_client.put_object(
                vnamespace,
                vbucket_name,
                file.replace(Files_For_Processing_Path,Files_After_Processing_Path), 
                get_object_response.data.content)
    print("Successfully copied object to folder Trade_Files_After_Processing")

    # Funtion to delete object from source bucket
    object_storage_client.delete_object(vnamespace, vbucket_name, file)
    print("Successfully deleted object from silver bucket")


if __name__ == "__main__":   
    main()