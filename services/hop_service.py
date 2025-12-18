import requests

from config.base import settings
from bs4 import BeautifulSoup
from loguru import logger

class HopService: 

    def __init__(self):
        self.__host: str = settings.APACHE_HOP_HOST
        self.__port: str = settings.APACHE_HOP_PORT
        self.__username: str = settings.APACHE_HOP_USER
        self.__password: str = settings.APACHE_HOP_PASS

    def get_pipeline(self): 
        resp = requests.get(
            f"http://{self.__host}:{self.__port}/hop/status", 
            auth=(self.__username, self.__password)
        )
        soup = BeautifulSoup(resp.content, "html.parser")
        hop_table = soup.find(class_="hop-table")
        
        data_rows = hop_table.find_all("tr")
        results = []
        for row_raw in data_rows:
            for td_row in row_raw.find_all("td"):
                results.append(td_row.get_text())
        
        result_mapped = []
        total_pipeline = len(results) / 5
        slider = 0
        for index in range(int(total_pipeline)): 
            try: 
                result_mapped.append(
                    {
                        "name_pipeline": results[index+slider+0],
                        "id_pipeline": results[index+slider+1],
                        "status": results[index+slider+2],
                        "start_date": results[index+slider+3],
                        "last_log_time": results[index+slider+4]
                    }
                )
                slider += 4
            except Exception as err: 
                break
        return result_mapped
    
    def __process_delete(self, res: dict): 
        url = f"http://{self.__host}:{self.__port}/hop/removePipeline/?name={res['name_pipeline']}&id={res['id_pipeline']}"

        payload = {}
        response = requests.get(
            url,
            auth=(self.__username, self.__password),
            data=payload
        )
        print(response.text)
    
    def delete_pipeline(self, with_error: bool): 
        results = self.get_pipeline()

        already_delete = []
        for res in results: 
            try: 
                if with_error and res['status'] == "Finished (with errors)": 
                    self.__process_delete(res)
                    already_delete.append(res)
                    continue

                if res['status'] != "Finished":
                    continue
                self.__process_delete(res)
                already_delete.append(res)
            except Exception as e: 
                logger.error("Error: {}", str(e))
        return already_delete
