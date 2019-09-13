#imports
from pyspark import SparkContext
from pyspark.sql import SparkSession








class read_file_from_local():
    def __init__(self):
        # add variables here
        self.dataframe=""
        sefl.address


        #initializing spark
        self.spark_context= SparkContext().getOrCreate()
        self.spark_session=  pyspark.sql.SparkSession(self.spark_context)


    def read(self, address="", file_format="csv"):
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
            self.dataframe= self.spark_session.read.csv(path, inferSchema = True, header = True)
            return self.dataframe
        except Exception as e:
            return {"success":False,"message":e}


    def read_excel(self):
        try:
            self.dataframe= self.spark_session.read.csv(path, inferSchema = True, header = True)
            return self.dataframe
        except Exception as e:
            return {"success":False,"message":e}


    def read_parquet(self):
        try:
            self.dataframe= self.spark_session.read.load("file2.parquet")
            return self.dataframe
        except Exception as e:
            return {"success":False,"message":e}



