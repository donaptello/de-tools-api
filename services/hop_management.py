import xml.etree.ElementTree as ET
import xmltodict
import glob

from config.base import settings


class HopManagementService: 

    def __init__(self):
        self.__base_path: str = settings.APACHE_HOP_DIRECTORY_PROJECT

    def find_all(self): 
        blob_list = glob.glob(f"{self.__base_path}/**", recursive=True)
        return [
            blob.replace(f"{self.__base_path}/", "") for blob in blob_list 
            if blob.endswith('.hpl') or blob.endswith('.hwf')
        ]
    
    def find(self, path: str): 
        return [
            blob.replace(f"{self.__base_path}/", "") for blob in self.find_all() 
            if path in blob
        ]
    
    def read_file(self, path: str): 
        with open(f"{self.__base_path}/{path}", 'r') as file: 
            data = xmltodict.parse(file.read())
        return data
