import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.logs import logger
from pyspark.sql.functions import isnan, when, count, col, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType





#template only


class handle_null_values(object):
    def __init__(self):
        pass


    def run(self,df):
        
        try: 
            variables= self.get_variables_types(df)
            
            del_variables= self.delete_variables(df)
            self.update_metadata(delete_variables=del_variables)
            
            df= self.handle_categorical_variables(df, variables)
            df= self.handle_numerical_variables(df, variables)
            self.update_metadata(v)
            
            return df
                
        except Exception as e:
            logger.error(e)
              

    def get_variables_types(self,df):
        """numeric variables"""
        variables=df.dtypes

        return variables
    
    
    def delete_var_with_null_more_than(self,dataframe, percentage=30):
        """numeric variables"""
        total_rows= dataframe.count()
        all_columns= dataframe.columns

        dropped_columns = []

        describe_dataframe= dataframe.select([count(when(col(c).isNull() | col(c).contains("NULL") | col(c).contains("null") | col(c).contains("None") | col(c).contains("NONE") | col(c).contains("none"), c)).alias(c) for c in dataframe.columns]).toPandas()

        for c in all_columns:
            missing_values= int(describe_dataframe[c][0])

            percent_missing= (missing_values)/total_rows *100

            if percent_missing>percentage:
                dropped_columns.append(c)

        return dropped_columns
    
    
    def handle_categorical_variables(self, df, variables):
        for c in variables:
            if c[1]=="string":
                
                #initialize the model
                
                #start training with the subset of data.
                
                
                #start testing
                df= df.withColumn(c, col(c).cast(DoubleType()))

        return df
    
    def handle_numerical_variables(self, df, variables):
        for c in variables:
            
            if c[1]!="string":
                
                #initialize the model
                
                #start training with the subset of data.
                
                
                #start testing
                df= df.withColumn(c, col(c).cast(DoubleType()))

        return df
    
   

    def categorical_testng(self, df, variables):
        
        #test with data

        return df

    
    def numerical_testng(self, df, variables):
        
        #test with data

        return df
    
    def update_metadata(self,column_name=[]):
        
        # update meta data
        
        return True
   
