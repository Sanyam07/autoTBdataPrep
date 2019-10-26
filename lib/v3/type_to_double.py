from lib.v3.imports import *


class treat_url_variables(Transformer,DefaultParamsReadable, DefaultParamsWritable):

    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column=''):
        # super(change_type, self).__init__()

        self._setDefault(column= column)
        self.setColumn(column)

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


    def _transform(self,df):
        return df.withColumn(self.getColumn(), col(self.getColumn()).cast(DoubleType()))


