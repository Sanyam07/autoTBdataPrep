from lib.v3.imports import *
from lib.v3.drop import *
from lib.v3.correct_day_format import *
from lib.v3.type_to_double import *
from lib.v3.handle_null_values import *
from lib.v3.drop_col_with_same_val import *
from lib.v3.find_time_variables import *
from lib.v3.correct_variable_types import *
from lib.v3.removing_duplication_urls import *
from lib.v3.treat_url_variables import *
from lib.v3.type_to_double import *


class transform_pipeline():
    def __init__(self, ID="", key="", local=True, s3=False):
        # variables
        self.pipeline = None
        self.stages = []
        # selected_columns specifies the order of the columns
        self.param = {"local": local, "s3": s3}

        done = True
        while done:
            self.sc = pyspark.SparkContext.getOrCreate()
            if self.sc == None:
                print("something")
                time.sleep(2)
            else:
                done = False

        # configuration
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if local == False:
            self.hadoop_conf.set("fs.s3n.awsAccessKeyId", ID)
            self.hadoop_conf.set("fs.s3n.awsSecretAccessKey", key)
        self.sql = pyspark.sql.SparkSession(self.sc)

    # methods
    def set_parameter(self, key, value):
        self.param[key] = value

    def get_parameter(self, key=None):
        if key == None:
            return self.param
        return self.param[key]

    # methods
    def save_pipeline(self, bucket='mltrons', path=None):
        if path == None:
            # generate own path
            path = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))

        if self.param["s3"] == True:
            self.pipeline.save('s3n://' + bucket + '/' + path)

        if self.param["local"] == True:
            self.pipeline.save(path)

        return path

    def load_pipeline(self, df=None, bucket='mltrons', path=None):
        if df == None:
            print("PLease provide test df")
            return False
        if self.param["s3"] == True:
            pipeline = Pipeline.load('s3n://' + bucket + '/' + path)
        if self.param["local"] == True:
            pipeline = Pipeline.load(path)

        self.pipeline = pipeline.fit(df)
        return self.pipeline

    def remove_y_from_variables(self, variables, y_var, training=True):
        new_variable = []
        if training == True:
            for v in variables:
                if y_var == v[0]:
                    pass
                else:
                    new_variable.append(v)
        return new_variable

    def convert_y_to_float(self, df, y_var):
        return df.withColumn(y_var, col(y_var).cast(DoubleType()))

    def transform(self, df=None):
        if df == None:
            print("Please provide dataframe to build")
            return False

        if self.pipeline == None:
            print("Please build or load the pipeline first.")
            return False
        df = self.pipeline.transform(df)
        return df

    def build_pipeline(self, df=None):

        if df == None:
            print("Please provide dataframe to build a pipeline")
            return False

        """1. Find variables with 70% or more null values"""
        try:
            variables = self.variables_with_null_more_than(df, percentage=30)
        except Exception as e:
            print(e, "in finding columns with a lot of missing values. 1")
            return False
        # Drop all these variables.
        try:
            self.drop_these_variables(variables)
        except Exception as e:
            print(e, "in dropping variables. 1")
            return False

        print("1. Find variables with 70% or more null values")

        """ 2. Find which varaible contains time and what the format of time is"""
        try:
            time_variables = self.find_all_time_variables(df)
        except Exception as e:
            print(e, "in finding time variables. 2")
            return False
        # handle time
        try:
            self.split_change_time(time_variables)
        except Exception as e:
            print(e, "in split time. 2")
            return False

        print("2. Find which varaible contains time and what the format of time is")

        """3. Find all variables with single value"""
        try:
            variables = self.variables_with_same_val(df)
        except Exception as e:
            print(e, "in finding columns with only one value. 3")
            return False
        # Drop all these variables.
        try:
            self.drop_these_variables(variables)
        except Exception as e:
            print(e, "in dropping variables. 3")
            return False

        print("3. Find all variables with single value")

        """4. convert variable type """
        try:
            int_variables = self.correct_variable_types(df)
            self.param["int_variables"] = int_variables  # just saving this for future use.
        except Exception as e:
            print(e, "in finding int variables saved as strings. 4")
            return False
        # Change type
        try:
            self.int_to_double(df.dtypes, int_variables)
        except Exception as e:
            print(e, "int to double. 4")
            return False

        print("4. convert variable type")

        """5. Treat duplications"""
        try:
            url_variables = self.find_variables_containing_urls(df)
            print("done")
        except Exception as e:
            print(e, "in finding vraibales containing urls. 5")
            return False
        try:
            var = self.clean_variable_containing_urls(url_variables)
        except Exception as e:
            print(e, "in fixing variables containing urls. 5")
            return False

        print("5. Treat duplications")

        """6. Treat missing values in numeric variables."""
        try:
            numeric_variables = self.find_numeric_variables(df.dtypes, int_variables)
            print("done")
        except Exception as e:
            print(e, "in finding all numeric variables. 6")
            return False
        try:
            var = self.handle_missing_values(numeric_variables)
        except Exception as e:
            print(e, "in filling numeric variables with mean.")
            return False

        print("6. Treat missing values in numeric variables")

        """Initialize spark pipeline."""
        pi = Pipeline(stages=self.stages)

        self.pipeline = pi.fit(df)
        return self.pipeline

    def find_variables_containing_urls(self, df):

        """Find all variables containing urls"""
        n = removing_duplication_urls()
        variables = n.fetch_columns_containing_url(df)
        return variables

    def clean_variable_containing_urls(self, df, variables=[]):

        """Clean all the variables containing urls"""
        for v in variables:
            d = treat_url_variables(column=v)
            self.stages += [change]

        return True

    def variables_with_null_more_than(self, df, percentage=20):

        """Find all the variables that contain null values more than 20%"""
        n = handle_null_values()
        variables = n.delete_var_with_null_more_than(df, percentage=percentage)
        return variables

    def correct_variable_types(self, df):

        """Find all numeric variables saved as string."""
        n = correct_variable_types()
        variables = n.find_numeric_variables_saved_as_string(df)
        return variables

    def find_all_time_variables(self, df, percentage=20):

        """Find all variables that contain time"""
        n = find_time_variables()
        variables = n.run(df)
        return variables

    def variables_with_same_val(self, df):

        """find variables that contain save value."""
        n = drop_col_with_same_val()
        variables = n.run(df)
        return variables

    def drop_these_variables(self, variables):
        for v in variables:
            d = drop(column=v)
            self.stages += [d]

    def int_to_double(self, dtypes, int_variables):
        co = ['bigint', 'int', 'double', 'float']

        for colum in dtypes:
            if colum[1] in co:
                # time to transform
                change = change_type(column=colum[0])
                self.stages += [change]
            elif colum[1] in int_variables:
                # time to transform
                change = change_type(column=colum[0])
                self.stages += [change]

    def find_numeric_variables(self, dtypes, int_variables):
        co = ['bigint', 'int', 'double', 'float']

        numeric_variables = []
        for colum in dtypes:
            if colum[1] in co:
                numeric_variables.append(colum[0])
            elif colum[1] in int_variables:
                numeric_variables.append(colum[0])
        return numeric_variables

    def encode_categorical_var(self):

        for column in self.param["columns"]:
            if self.param['time_variable'] != []:
                if column[0] == self.param['time_variable'][0]:
                    continue
            if column[1] == 'string':
                # time to transform
                stringIndexer = StringIndexer(inputCol=column[0], outputCol=column[0] + "Index").setHandleInvalid(
                    "keep")
                self.param["selected_columns"].append(column[0] + "Index")
                self.stages += [stringIndexer]
                d = drop(column[0])
                self.stages += [d]
            else:
                self.param["selected_columns"].append(column[0])

    def handle_missing_values(self, numeric_variables):

        imputer = Imputer(
            inputCols=numeric_variables,
            outputCols=numeric_variables
        )
        self.stages += [imputer]

    def split_change_time(self, time_variables):
        for v in time_variables:
            time = correct_day_format(column=v)
            self.stages += [time]

            # To Do


""" 
Pipeline 1 remaining steps
        
        # 7. Find which variables contain skewness
        # Treat skewed variables.
        
        # 8. Find which variables are important.
        # Drop unimportant variables.
        
"""

"""
Pipeline 2

        # Imputations of categorical variable using datawig.

"""

"""
Pipeline 3
      
        # 9. Encode categorical variables
        try:
            
            self.encode_categorical_var()
        except Exception as e:
            print(e, "categorical to float")
            return False

            
            
        # 10. Find the order of all variables.
        # Rearrange dataframe using above order.

"""
