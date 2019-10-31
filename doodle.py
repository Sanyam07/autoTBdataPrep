"""
self.param = {"local": local, "s3": s3, "dropped_variables": [], "selected_variables": [], "all_variables": [],
              "numerical_variables": [], "categorical_variables": []}

df = df.withColumn(time_variable + '_year', funct.year(new_time_variable))
df = df.withColumn(time_variable + '_month', funct.month(new_time_variable))
df = df.withColumn(time_variable + '_day', funct.dayofmonth(new_time_variable))
df = df.withColumn(time_variable + '_dayofweek', funct.dayofweek(new_time_variable))
df = df.withColumn(time_variable + '_hour', funct.hour(new_time_variable))
df = df.withColumn(time_variable + '_minutes', funct.minute(new_time_variable))
df = df.withColumn(time_variable + '_seconds', funct.second(new_time_variable))

ls = ['_year', '_month', '_day', '_dayofweek', '_hour', '_minutes', '_seconds']

pi = Pipeline(stages=self.stages)
self.pipeline = pi.fit(df)
return self.pipeline
"""
