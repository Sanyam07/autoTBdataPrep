import pyspark.sql.functions as funct
from pyspark.ml import Transformer
from lib_v2.logs import logger


class ColumnsDroppingSameValue(Transformer):

    def __init__(self):
        super(ColumnsDroppingSameValue, self).__init__()

    def _transform(self, df):
        """
        remove columns which contains only one kind of value
        :param df: original dataframe containing data
        :return: return dataframe after removing columns
        """
        try:

            col_counts = df.select([(funct.countDistinct(funct.col(col))).alias(col) for col in df.columns]).collect()[
                0].asDict()
            to_drop = [k for k, v in col_counts.items() if v == 1]

            logger.warn("Columns to drop")
            logger.warn(to_drop)

            df = df.drop(*to_drop)
            return df
        except Exception as e:
            logger.error(e)
