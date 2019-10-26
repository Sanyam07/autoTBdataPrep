from lib.v2.imports import *
from lib.v2.Logger.logs import logger
import pyspark.sql.functions as funct
from dateutil import parser


class DateTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column='', time_format='yyyy-MM-dd HH:mm:ss.SS'):

        super(DateTransformer, self).__init__()
        # lazy workaround - a transformer needs to have these attributes

        self._setDefault(column=column)
        self.setColumn(column)
        self.time_format = time_format

    def getColumn(self):

        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.column)

    def setColumn(self, value):
        """
        Sets the value of :py:attr:`withStd`.
        """
        return self._set(column=value)

    def _transform(self, df):

        time_variable = self.getColumn()
        new_time_variable = time_variable + '_new'

        # code from tawab. Convert all times in a same format.

        df = df.withColumn(new_time_variable, self.udf_date_formatting()(funct.col(time_variable).cast("String")))

        df = df.withColumn(new_time_variable,
                           from_unixtime(unix_timestamp(time_variable + '_new', self.time_format)).cast(
                               TimestampType()))

        df = df.withColumn(time_variable + '_year', year(new_time_variable))
        df = df.withColumn(time_variable + '_month', month(new_time_variable))
        df = df.withColumn(time_variable + '_day', dayofmonth(new_time_variable))
        df = df.withColumn(time_variable + '_dayofweek', dayofweek(new_time_variable))
        df = df.withColumn(time_variable + '_hour', hour(new_time_variable))
        df = df.withColumn(time_variable + '_minutes', minute(new_time_variable))
        df = df.withColumn(time_variable + '_seconds', second(new_time_variable))

        df = df.drop(new_time_variable)
        df = df.drop(time_variable)
        return df

    @staticmethod
    def date_formatting(x):
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


