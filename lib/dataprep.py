#imports
import pandas as pd





class data_prep():
    def __init__(self):
        # add variables here
        self.recipe= None
        self.datafame=None
        self.address= ""
        self.file_format="csv"
        self.output_address= ""
        self.output_format=""

        #initializing spark


    def prep(self, dataframe=None, s3={},local_address="",file_format="csv", output_address="", output_format="" ):

        #convert dataframe into spark dataframe if datarame provided is not none
        if dataframe != None:
            p= pd.DataFrame()
            if type(dataframe)== type(p):
                #convert dataframe into spark dataframe
                pass
            else
                self.dataframe= dataframe



        # read the file from local or s3 when dataframe passed is none
        if dataframe==None:
            self.dataframe= self.read_as_dataframe()




        return True

    def display_recipe(self):
        return True


    def save_recipe(self, address=""):
        return True


    def read_as_dataframe(self):
        return True

    def save_dataframe(self, address=""):
        return True

    def get_recipe(self):
        return self.recipe

    def set_recipe(self, recipe=None):
        self.recipe= recipe
        return True

    def prep_again(self, dataframe=None, s3={},local_address="",file_format="csv", output_address="", output_format="", recipe=None):
        return True
