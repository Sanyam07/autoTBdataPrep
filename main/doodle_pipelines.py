from lib_v2.columns_dropping_nan import ColumnsDroppingContainsNan
from lib_v2.columns_dropping_with_same_value import ColumnsDroppingSameValue
from pyspark import SparkContext
from pyspark.sql import SparkSession
from lib_v2.logs import logger, file_logs
from pyspark.ml import Pipeline
file_logs("mltrons")

s = SparkContext.getOrCreate()


sql = SparkSession(s)
def columns_same_value():
    try:
        df = sql.read.csv("./run/column_rem.csv", inferSchema=True, header=True)

        columns_with_same_val = ColumnsDroppingSameValue()
        model = Pipeline(stages=[columns_with_same_val]).fit(df)
        result = model.transform(df)

        result.toPandas().to_csv('./run/pipeline_same_value.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(result.show())
    except Exception as e:
        logger.error(e)


def remove_cols_containing_nan():
    try:

        logger.debug("this is debug")
        df = sql.read.csv("./run/rem_test.csv", inferSchema=True, header=True)

        return_df = Duplication().remove_columns_containing_all_nan_values(df)
        return_df.toPandas().to_csv('./run/rem_test_result.csv')
        print(df.show())
        print("#####################")
        print("resulted_df")
        print(return_df.show())
    except Exception as e:
        logger.error(e)


if __name__ =="__main__":
    columns_same_value()