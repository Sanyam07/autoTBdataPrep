import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.v3.logs import logger
from pyspark.sql.functions import isnan, when, count, col, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType
import datawig
import datetime





class drop_col_with_same_val(object):
    def __init__(self):
        self.variables=[]
        self.categorical_variables=[]
        self.numerical_variables=[]
        self.models={}
        self.df=None


    def run(self,df):
        
        """
        remove columns which contains only one kind of value
        :param df: original dataframe containing data
        :return: return dataframe after removing columns
        """
        try:

            col_counts = df.select([(funct.countDistinct(funct.col(col))).alias(col) for col in df.columns]).collect()[
                0].asDict()
            to_drop = [k for k, v in col_counts.items() if v == 1]

            logger.warn("Columns to drop")
            logger.warn(to_drop)

            return to_drop
        except Exception as e:
            logger.error(e)
              
