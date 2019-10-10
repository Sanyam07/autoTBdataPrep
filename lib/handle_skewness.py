import re
import sys
import pyspark.sql.functions as funct
from lib.logs import logger


class HandleSkewness(object):
    def __init__(self):
        pass

    def handle_neg_values(self, df):
        """

        :param df:
        :return:
        """
        min_value = df.agg({df.columns[0]: "min"}).collect()
        if min_value <= 0:
            df = df.withColumn(df.columns[0], funct.col(df.columns[0]) + abs(min_value) + 0.01)
            return df

    def remove_skewness(self, df, columns):
        """

        :param df:
        :param columns:
        :return:
        """
        for col in columns:
            skew_val = df.select(funct.skewness(df[col]))
            if skew_val < 0:
                self.handle_neg_values(df[col])
            elif skew_val > 0:
                pass
            else:
                return df
