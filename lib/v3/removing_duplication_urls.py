import pyspark.sql.functions as funct
from pyspark.ml import Transformer
from lib.v2.Logger.logs import logger
from nltk.corpus import stopwords

import re
from urllib.parse import urlparse


class removing_duplication_urls(Transformer):
    def __init__(self, base_url=''):
        super(removing_duplication_urls, self).__init__()
        self.stop_words = stopwords.words('english')
        self.base_url = base_url

    @staticmethod
    def fetch_columns_containing_url(df):
        """
        Automatically fetch column name contains urls
        :param df: orig dataframe
        :return: return list of columns containing urls
        """
        try:
            col_dict = df.select([funct.col(col).rlike(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))').alias(col) for col in
                                  df.columns]).collect()[0].asDict()
            col_containing_url = [k for k, v in col_dict.items() if v is True]
            return col_containing_url
        except Exception as e:
            logger.error(e)

    def removing_stop_words(self, x, base_url):
        """
        url column and base_url is given and cleaned url is returned

        :param x: row on which cleaning is need to be performed
        :param base_url: Contains base_url
        :return: cleaned url
        """
        try:
            # If base_url param is empty figure out base_url using urllib
            if x is not None:
                if base_url == '':
                    base_url = urlparse(x)
                    base_url = base_url.netloc if base_url.scheme != '' else base_url.path.split("/")[0]

                res = x.replace("https://", "").replace("http://", "").replace(base_url, "")

                # fetch only alphabets ignore all special characters
                tokens = re.findall(r"[\w:']+", res)

                # remove duplicate words from url
                tokens = list(dict.fromkeys(tokens))

                # remove stop words from url
                elem = []
                if len(tokens) > 0:
                    elem = [word for word in tokens if word not in self.stop_words]

                # add base_url to the url
                elem.insert(0, base_url)
                new_url = '/'.join(elem)

                return new_url
        except Exception as e:
            logger.error(e)
            return x

    def udf_remove_stop_words(self, base_url):
        """
        a run function to create a udf function with default params
        :param base_url: contains base_url if any
        :return: cleaned url
        """
        return funct.udf(lambda x: self.removing_stop_words(x, base_url))

    def remove_duplicate_urls(self, df, column_name):
        """
        Orig dataframe is received  with columns containing urls .
        Those columns are cleaned to remove duplication

        :param df: dataframe containing data which need to be cleaned
        :param column_name: list of columns containing urls
        :return: return dataframe with _new column name append describing cleaned column
        """
        try:
            for i in column_name:
                df = df.withColumn(i + '_new',
                                   self.udf_remove_stop_words(self.base_url)(
                                       funct.trim(funct.lower(funct.col(i).cast("string")))))

            return df
        except Exception as e:
            logger.error(e)

    def _transform(self, df):
        try:
            columns_list = self.fetch_columns_containing_url(df)

            logger.warn("columns contains urls : ")
            logger.warn(columns_list)

            result = self.remove_duplicate_urls(df, columns_list)
            return result
        except Exception as e:
            logger.error(e)
