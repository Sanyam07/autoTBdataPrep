#imports
from read_file_from_local import read_file_from_local
from read_file_from_s3 import read_file_from_s3




class read_file():
    def __init__(self):
        # add variables here
        self.dataframe=""


    def read(self, address="", local= "no", file_format="csv", s3={}):
        if local=="yes":
            """
            Time to read the file saved locally
            """
            self.dataframe= read_file_from_local(address,file_format)

        elif s3!={}:
            """
            Time to read data from s3
            """
            self.dataframe =read_file_from_s3(address, file_format, s3)

        else:
            """
            Not sure where the file is saved.
            """
            message= "Please make sure you have file saved on either your local system or s3."
            print(message)
            self.dataframe = {"success": False, "message": message }

        return self.dataframe
