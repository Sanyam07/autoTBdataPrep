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

        :param df:
        :return:
        """
        min_value = df.agg({df.columns[0]: "min"}).collect()[0][0]
        if min_value <= 0:
            df = df.withColumn(df.columns[0], funct.col(df.columns[0]) + abs(min_value) + 0.01)
        return df

    def apply_box_cox(self, x):
        """

        :param x:
        :return:
        """

        return float(round(stats.boxcox([x])[0][0], 2))

    def apply_log(self, x):
        """

        :param x:
        :return:
        """
        try:
            return float(round(np.log(x), 3))
        except Exception as e:
            print(e)
            print(x)

    def udf_box_cox(self):
        """

        :return:
        """
        return funct.udf(lambda x: self.apply_box_cox(x), FloatType())

    def udf_log(self):
        return funct.udf(lambda x: self.apply_log(x), FloatType())

    def remove_skewness(self, df, columns, threshold=0.7):
        """

        :param df:
        :param columns:
        :return:
        """
        for col in columns:
            skew_val = df.select(funct.skewness(df[col])).collect()[0][0]

            if abs(skew_val) > threshold and skew_val < 0:
                df = self.handle_neg_values(df[[col]])
                df = df.withColumn(col, self.udf_box_cox()(funct.col(col)))
                
            elif abs(skew_val) > threshold and skew_val > 0:
                df = self.handle_neg_values(df[[col]])
                df = df.withColumn(col, self.udf_log()(funct.col(col)))

        return df
