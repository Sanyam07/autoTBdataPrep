import sys
# from urllib.parse import urlparse,urlsplit
# parsed_uri = urlparse('https://www://stackoverflow.com/www://stackoverflow.com/questions/1234567/blah-blah-blah-blah' )
# #urllib.parse.urlsplit(x)
# import re
#
# myString = "http://example.com/blah"
#
#
# # result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
# print(parsed_uri)
#
# res = ['index']
# abc ="www.mltrons.com"+'/'+'/'.join(res)
# print(abc)
# sys.exit()

from lib.duplication import *
from pyspark import SparkContext
from pyspark.sql import SparkSession

s = SparkContext.getOrCreate()

sql = SparkSession(s)

df = sql.read.csv("./run/column_rem.csv", inferSchema=True, header=True)
print(df.columns)
return_df = Duplication().remove_columns_contains_same_value(df)
return_df.toPandas().to_csv('./run/rem_test.csv')
print(df.show())
print("#####################")
print("resulted_df")
print(return_df.show())


