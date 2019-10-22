import pyspark.sql.functions as funct
from pyspark.ml import Transformer
from lib_v2.logs import logger


class ColumnsDroppingContainsNan(Transformer):
    """
    A custom Transformer remove columns which contains Nan greater then a threshold
    
    """

    def __init__(self, threshold=30):
        super(ColumnsDroppingContainsNan, self).__init__()
        self.threshold = threshold

    def _transform(self, df):
        """
        Transform functions which remove columns containing more then 30 percent of nans
        :param df:
        :return:
        """
        try:
            null_counts = \
                df.select(
                    [funct.count(funct.when(funct.col(col).isNull() |
                                            funct.col(col).contains("NULL") |
                                            funct.col(col).contains("null") |
                                            funct.col(col).contains("Null") |

                                            funct.col(col).contains("None") |
                                            funct.col(col).contains("NONE") |
                                            funct.col(col).contains("none"), col)).alias(col) for col in
                     df.columns]).collect()[0].asDict()
            size_df = df.count()
            to_drop = [k for k, v in null_counts.items() if ((v / size_df) * 100) >= self.threshold]
            logger.warn("columns to drop :")
            logger.warn(to_drop)
            df = df.drop(*to_drop)
            return df
        except Exception as e:
            logger.error(e)
