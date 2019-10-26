from lib.v2.imports import *


class FetchSkewedCol(object):
    def __init__(self,threshold=0.7):
        self.skewed_col = []
        self.threshold = threshold

    def skewed_features(self, df):
        """

        :param df:
        :return:
        """
        try:
            for col in df.columns:
                skew_val = df.select(funct.skewness(df[col])).collect()[0][0]
                if skew_val is not None:
                    if abs(skew_val) > self.threshold and skew_val < 0:
                        self.skewed_col.append(col)
                    elif abs(skew_val) > self.threshold and skew_val > 0:
                        self.skewed_col.append(col)

            logger.warn("Skewed Columns are:")
            logger.warn(self.skewed_col)
            return self.skewed_col
        except Exception as e:
            logger.error(e)
            return self.skewed_col
