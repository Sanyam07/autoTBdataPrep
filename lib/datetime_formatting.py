import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.logs import logger


class DatetimeFormatting(object):
    def __init__(self):
        pass

    # def date_formatting(self, row):
    #     try:
    #         print("inside_date")
    #         if re.match(r"^\d{8}$", row):
    #             dateObj = datetime.datetime.strptime(row, '%Y%m%d')
    #
    #         elif re.match(r"^\d{1,2}/", row):
    #             dateObj = datetime.datetime.strptime(row, '%m/%d/%Y')
    #         elif re.match(r"^[a-z]{3}", row, re.IGNORECASE):
    #             dateObj = datetime.datetime.strptime(row, '%b %d %Y')
    #         elif re.match(r"^\d{1,2} [a-z]{3}", row, re.IGNORECASE):
    #             dateObj = datetime.datetime.strptime(row, '%d %b %Y')
    #         else:
    #             print("no regex applied")
    #             logger.debug("No regex applied")
    #             dateObj = row
    #         return str(dateObj)
    #     except Exception as e:
    #         logger.error(e)

    def date_formatting(self, x):
        return str(parser.parse(x))

    def udf_date_formatting(self):
        return funct.udf(lambda row: self.date_formatting(row))

    def date_cleaning(self, df, column_name=[]):
        try:
            for i in column_name:
                df = df.withColumn(i + '_new', self.udf_date_formatting()(funct.col(i).cast("String")))

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def fetch_columns_containing_datetime(df):

        try:
            col_dict = df.select([funct.col(col).rlike(r'(\d+(/|-){1}\d+(/|-){1}\d{2,4})').alias(col) for col in
                                  df.columns]).collect()[0].asDict()
            col_containig_url = [k for k, v in col_dict.items() if v is True]
            return col_containig_url
        except Exception as e:
            logger.error(e)
