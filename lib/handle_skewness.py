import re
import sys
import pyspark.sql.functions as funct
from lib.logs import logger
from scipy import stats
import numpy as np
from pyspark.sql.types import FloatType


class HandleSkewness(object):
    def __init__(self):
        pass

    def handle_neg_values(self, df):
        """
        Minimum value of columns is fetched
        zero/negative values are handled to remove skewness
        :param df: orig dataframe
        :return: updated column to remove skewness
        """
        try:
            min_value = df.agg({df.columns[0]: "min"}).collect()[0][0]
            logger.info("min_value is {}".format(min_value))
            if min_value <= 0:
                df = df.withColumn(df.columns[0], funct.col(df.columns[0]) + abs(min_value) + 0.01)
            return df
        except Exception as e:
            logger.error(e)

    def apply_box_cox(self, x):
        """
        boxcox function is applied to remove skewness
        :param x: row wise values of a column
        :return: updated value with minimum skewness
        """

        return float(round(stats.boxcox([x])[0][0], 2))

    def apply_log(self, x):
        """
        log function is applied to remove skewness
        :param x: row wise values of a column
        :return: updated value with minimum skewness
        """
        try:
            return float(round(np.log(x), 3))
        except Exception as e:
            logger.error(e)

    def udf_box_cox(self):
        """
        udf function to call boxcox
        :return: less skewed values
        """
        return funct.udf(lambda x: self.apply_box_cox(x), FloatType())

    def udf_log(self):
        """
        udf function to call log function
        :return:less skewed values
        """
        return funct.udf(lambda x: self.apply_log(x), FloatType())

    def remove_skewness(self, df, columns, threshold=0.7):
        """
        skewness is minimized using boxcox and log functions
        if skewness is negative boxcox is used to remove skewness
        if skewness is positive log is used to remove skewness
        if skewness is less then threshold remove skewness is not called

        :param df:orig dataframe
        :param columns: list of columns on which skewness is needed to removed
        :return: less skewed dataframe
        """
        try:
            for col in columns:
                skew_val = df.select(funct.skewness(df[col])).collect()[0][0]

                if abs(skew_val) > threshold and skew_val < 0:
                    df = self.handle_neg_values(df[[col]])
                    df = df.withColumn(col, self.udf_box_cox()(funct.col(col)))

                elif abs(skew_val) > threshold and skew_val > 0:
                    df = self.handle_neg_values(df[[col]])
                    df = df.withColumn(col, self.udf_log()(funct.col(col)))

            return df
        except Exception as e:
            logger.error(e)
