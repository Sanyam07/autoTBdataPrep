# imports

from lib.v2.Pipelines.etl_pipeline import *
from lib.v2.Operations.readfile import ReadFile as rf
from lib.v2.Logger.logs import logger, file_logs
from lib.v2.Transformers.date_transformer import DateTransformer
import numpy as np

file_logs("mltrons")

def testing_pipeline():
    r = rf()
    df3 = r.read(address='./run/4alan_data_clean.csv')
    # drop address
    df3 = df3.drop("Location")
    p1 = EtlPipeline()
    p1.build_pipeline(df3)


# def date_conversion():
#     r = rf()
#     df3 = r.read(address='./run/4alan_data_clean.csv')
#     df3 = df3.select(['Date_NEW'])
#     print(df3.columns)
#     datetime_formatting = DateTransformer()
#     model = Pipeline(stages=[datetime_formatting]).fit(df3)
#     res = model.transform(df3)
#     print("DDFDfdfd")
#     print("resulted_df")
#     print(res.show())


if __name__=='__main__':
    testing_pipeline()