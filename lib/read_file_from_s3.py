from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3


#imports






class read_file_from_s3():
    def __init__(self):
        # add variables here
        self.s3={}
        self.dataframe


    def read(self, address="", file_format="csv", s3={}):
        status, message= self.init_spark_with_s3(s3)
        if status== False:
            retun {"success": status, "message":message}

        self.s3=s3
        self.address= address

        if file_format== "csv":
            self.dataframe= self.read_csv()
        elif file_format=="excel":
            self.dataframe= self.read_excel()
        elif file_format=="parquet":
            self.dataframe= self.read_parquet()
        else:
            print("File format "+file_format+" is currently not supported. Please create a feature request on Github")

        return self.dataframe



    def read_csv(self):
        try:
            self.dataframe= self.spark_session.read.csv('s3n://'+self.s3["bucket"]+'/'+ path, inferSchema = True, header = True)
            return self.dataframe
        except Exception as e:
            return {"success":False,"message":e}

    def read_excel(self):
        try:
            self.dataframe= self.spark_session.read.csv('s3n://'+self.s3["bucket"]+'/'+ path, inferSchema = True, header = True)
            return self.dataframe
        except Exception as e:
            return {"success":False,"message":e}

    def read_parquet(self):
        try:
            self.dataframe= self.spark_session.read.load('s3n://'+self.s3["bucket"]+'/'+ path)
            return self.dataframe
        except Exception as e:
            return {"success":False,"message":e}


    def init_spark_with_s3(self, s3):

        success= False
        message=""

        from i in range(10):

            try:

                self.spark_context= SparkContext().getOrCreate()

                self.hadoop_conf=self.spark_context._jsc.hadoopConfiguration()
                self.hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
                self.hadoop_conf.set("fs.s3n.awsAccessKeyId", s3["ID"])
                self.hadoop_conf.set("fs.s3n.awsSecretAccessKey", s3["key"])

                self.spark_session=  pyspark.sql.SparkSession(self.spark_context)
                success= True
                continue
             except Exception as e:
                message=e
                pass

        return success, message
