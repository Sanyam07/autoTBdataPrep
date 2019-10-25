import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from logs import logger
from pyspark.sql.functions import isnan, when, count, col, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType
import datawig
import datetime





class find_time_variables(object):
    def __init__(self):
        self.variables=[]
        self.categorical_variables=[]
        self.numerical_variables=[]
        self.models={}
        self.df=None
              

    
    
    def run(self, df):
        """
        Automatically detects the column which contains the date values
        :param df: orig dataframe
        :return: list of column name contains the date values
        """
        try:
            col_dict = df.select([funct.col(col).rlike(r'(\d+(/|-){1}\d+(/|-){1}\d{2,4})').alias(col) for col in
                                  df.columns]).collect()[0].asDict()
            col_containig_dates = [k for k, v in col_dict.items() if v is True]

            logger.warn("columns cotanins date:")
            logger.warn(col_containig_dates)

            return col_containig_dates
        except Exception as e:
            logger.error(e)
            return []