import pyspark.sql.functions as funct
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
import pandas as pd


class ColumnsDroppingContainsNan(Transformer):
    """
    A custom Transformer remove columns which contains Nan greater then a threshold
    
    """

    def __init__(self, threshold):
        super(ColumnsDroppingContainsNan, self).__init__()
        self.threshold = threshold

    def _transform(self, df):
        """

        :param df:
        :return:
        """
        