#imports






class save_file_s3():
    def __init__(self):
        # add variables here
        self.temp=""


    def save(self, address="", file_format="csv", s3={}):
        self.s3= s3

        return True

    def save_csv(self):
        return True

    def save_excel(self):
        return True

    def save_parquet(self):
        return True



