import pyspark.sql.functions as funct
from pyspark.ml import Transformer
from lib_v2.logs import logger
from scipy import stats
import numpy as np
from pyspark.sql.types import FloatType


class MinimizeSkewness(Transformer):
    def __init__(self, threshold=0.7):
        super(MinimizeSkewness, self).__init__()
        self.threshold = threshold

    @staticmethod
    def handle_neg_values(df,column_name):
        """
        Minimum value of columns is fetched
        zero/negative values are handled to remove skewness
        :param df: orig dataframe
        :return: updated column to remove skewness
        """
        try:
            min_value = df.agg({column_name: "min"}).collect()[0][0]
            logger.info("min_value is {}".format(min_value))
            if min_value <= 0:
                df = df.withColumn(column_name, funct.col(column_name) + abs(min_value) + 0.01)
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

    def remove_skewness(self, df, columns):
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
            for col_name in columns:

                skew_val = df.select(funct.skewness(df[col_name])).collect()[0][0]
                if skew_val is not None:
                    if abs(skew_val) > self.threshold and skew_val < 0:

                        df = self.handle_neg_values(df,col_name)
                        df = df.withColumn(col_name, self.udf_box_cox()(funct.col(col_name)))

                    elif abs(skew_val) > self.threshold and skew_val > 0:
                        df = self.handle_neg_values(df,col_name)
                        df = df.withColumn(col_name, self.udf_log()(funct.col(col_name)))

            return df
        except Exception as e:
            logger.error(e)

    def skewed_features(self,df):
        """

        :param df:
        :return:
        """
        try:
            skewed_features = []
            for col in df.columns:
                skew_val = df.select(funct.skewness(df[col])).collect()[0][0]
                if skew_val is not None:
                    if abs(skew_val) > self.threshold and skew_val < 0:
                        skewed_features.append(col)
                    elif abs(skew_val) > self.threshold and skew_val > 0:
                        skewed_features.append(col)

            logger.warn("Skewed Columns are:")
            logger.warn(skewed_features)
            return skewed_features
        except Exception as e:
            logger.error(e)

    def _transform(self, df):

        columns = self.skewed_features(df)
        res = self.remove_skewness(df, columns)
        return res
