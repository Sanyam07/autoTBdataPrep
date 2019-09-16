from lib.duplication import *
from pyspark import SparkContext
from pyspark.sql import SparkSession

s = SparkContext.getOrCreate()

sql = SparkSession(s)

df = sql.read.csv("./run/testing_dup.csv", inferSchema=True, header=True)
print(df.columns)
return_df = Duplication().remove_duplicate_ids(df,['duplicate_id','random_col','id'])

print(df.show())
print("#####################")
print("resulted_df")
print(return_df.show())