from imports import *



class correct_day_format(Transformer,DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)
    formt = Param(Params._dummy(), "formt", "format", typeConverter=TypeConverters.toString)

    def __init__(self, column='', formt=''):
        super(correct_day_format, self).__init__()
        # lazy workaround - a transformer needs to have these attributes
#         self._defaultParamMap = dict()
#         self._paramMap = dict()
        self._setDefault(formt=formt, column= column)
        self.setColumn(column)
        self.setFormt(formt)


#         self.uid= str(uuid.uuid4())


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

    def getFormt(self):
        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.formt)


    def setFormt(self, value):
        """
        Sets the value of :py:attr:`withStd`.
        """
        return self._set(formt=value)


    def _transform(self,df):
        df= df.withColumn(self.getColumn(), from_unixtime(unix_timestamp(self.getColumn(), self.getFormt())).cast(TimestampType()))

        df= df.withColumn('yearrr', year(self.getColumn()))
        df= df.withColumn('monthhh', month(self.getColumn()))
        df= df.withColumn('dayyy', dayofmonth(self.getColumn()))
        df= df.withColumn('dayofweekkk', dayofweek(self.getColumn()))
        df= df.withColumn('hourrr', hour(self.getColumn()))
        df= df.withColumn('minutesss', minute(self.getColumn()))
        df= df.withColumn('secondss', second(self.getColumn()))
        df= df.drop(self.getColumn())
        return df