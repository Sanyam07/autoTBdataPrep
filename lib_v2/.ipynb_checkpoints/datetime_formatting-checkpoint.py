import pyspark.sql.functions as funct
from pyspark.ml import Transformer
from lib_v2.logs import logger
from dateutil import parser


class DatetimeFormatting(Transformer):

    def __init__(self):
        super(DatetimeFormatting, self).__init__()

    @staticmethod
    def fetch_columns_containing_datetime(df):
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

    def date_formatting(self, x):
        """
        dateutill library is used to convert the different format of dates into standard format
        :param x: row wise date values
        :return: standard format of date
        """
        try:
            return str(parser.parse(x))
        except Exception as e:
            logger.error(e)
            return str(x)

    def udf_date_formatting(self):
        """
        Run function calls the main date_formatting function
        :return: standard format of date
        """
        return funct.udf(lambda row: self.date_formatting(row))

    def date_cleaning(self, df, column_name=[]):
        """
        Converts all the columns containing dates into standard date format
        In a for loop every column values are traverse and udf_date_formatting function is called

        :param df: orig dataframe
        :param column_name: list of column names containing date
        :return: return a new_df containing some new columns with updated date values
        """
        try:
            for i in column_name:
                df = df.withColumn(i + '_new', self.udf_date_formatting()(funct.col(i).cast("String")))
            return df
        except Exception as e:
            logger.error(e)

    def _transform(self, df):
        col_list = self.fetch_columns_containing_datetime(df)
        res = self.date_cleaning(df, col_list)
        return res
