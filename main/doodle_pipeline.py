# imports

from lib.v2.Pipelines.etl_pipeline import *
from lib.v2.Operations.readfile import ReadFile as rf
from lib.v2.Logger.logs import logger, file_logs
import numpy as np

file_logs("mltrons")

def testing_pipeline():
    r = rf()
    df3 = r.read(address='./run/4alan_data_clean.csv')
    # drop address
    df3 = df3.drop("Location")
    p1 = EtlPipeline()
    p1.build_pipeline(df3)


if __name__=='__main__':
    testing_pipeline()