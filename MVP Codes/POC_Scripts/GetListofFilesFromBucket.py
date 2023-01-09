import pandas as pd
import oci 
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from io import StringIO



def createFile_List():
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    #function to get object from source bucket
    list_objects_response = object_storage_client.list_objects(
        namespace_name=vnamespace,
        bucket_name=vbucket_name,
        prefix = "Trade_Files_For_Processing/part")

    filelist = []
    for o in list_objects_response.data.objects:
        print(o.name)
        filelist.append('oci://ratd_silver_bucket@apaciaas/' + o.name)
        # Load the input files into spark dataframes        
        print(o.name.replace("Trade_Files_For_Processing", "Trade_Files_After_Processing"))
        print(o.name)

    filelist_df = pd.DataFrame(filelist, columns=["File_Names"])
    filelist_df.to_csv('FileList.csv',header =True, index=False)

    print("File list created successfully!")
  


def upload_file():
    print("\t Start - in upload_file method") 
   
    object_storage = oci.object_storage.ObjectStorageClient(config)
    namespace = vnamespace
    bucket_name = vbucket_name   
    fp = open(vobject_name,'rb')
    obj = object_storage.put_object(
        namespace,
        bucket_name,
        vobject_name,
        fp)
    print("\t\t FileList.csv uploaded Successfully")
    print("\t End - in upload_file method")

# Main function that calls all the sub functions
if __name__ == '__main__':
    ociconfig = oci.config.from_file()
    config = oci.config.from_file()
    vnamespace = "apaciaas"
    vbucket_name = "ratd_silver_bucket"
    vobject_name = "FileList.csv"
    createFile_List()
    #upload_file()
 


#df_list = (spark_session.read.format('csv').option("header","true").load(file) for file in filelist)
# Concatenate all DataFrames
#tradedata_df   = pd.concat(df_list, ignore_index=True)

#for i in range(1,len(filelist)):
#    tradedata1_df = spark_session.read.format('csv').option("header","true").load(filelist[i])
 #    tradedata_df = pd.concat([tradedata_df,tradedata1_df],axis=1)
#tradedata_df = spark_session.read.format('csv').option("header","true").load('oci://ratd_silver_bucket@apaciaas/Trade_Files/*.csv')
#tradedata_df = spark_session.read.format('csv').option("header","true").load(filelist)
#tradedata_df.show()