import requests
 
from config.base import settings
from bs4 import BeautifulSoup
from loguru import logger
 
class HopService:
 
    def __init__(self, mode: str):
        self.__host: str = settings.APACHE_HOP_HOST
        self.__port: str = settings.APACHE_HOP_PORT
        self.__username: str = settings.APACHE_HOP_USER
        self.__password: str = settings.APACHE_HOP_PASS
        self.__mode: str = mode
 
    def get_pipeline(self):
        resp = requests.get(
            f"http://{self.__host}:{self.__port}/hop/status",
            auth=(self.__username, self.__password)
        )
        soup = BeautifulSoup(resp.content, "html.parser")
        hop_tables = soup.find_all("table", {"class": "hop-table"})
       
        results_all_mapped = []
        for index_type, hop_table in enumerate(hop_tables):
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
                    if index_type == 0:
                        result_mapped.append(
                            {
                                "name_pipeline": results[index+slider+0],
                                "id_pipeline": results[index+slider+1],
                                "status": results[index+slider+2],
                                "start_date": results[index+slider+3],
                                "last_log_time": results[index+slider+4],
                                "type": "pipeline"
                            }
                        )
                    elif index_type == 1:
                        result_mapped.append(
                            {
                                "name_workflow": results[index+slider+0],
                                "id_workflow": results[index+slider+1],
                                "status": results[index+slider+2],
                                "start_date": results[index+slider+3],
                                "last_log_time": results[index+slider+4],
                                "type": "workflow"
                            }
                        )
                    slider += 4
                except Exception as err:
                    break
            results_all_mapped.append(result_mapped)
        if self.__mode == "Pipeline":
            return results_all_mapped[0]
        elif self.__mode == "Workflow":
            return results_all_mapped[1]
        return results_all_mapped
   
    def __process_delete_pipeline(self, res: dict):
        url = f"http://{self.__host}:{self.__port}/hop/removePipeline/?name={res['name_pipeline']}&id={res['id_pipeline']}"
        payload = {}
        response = requests.get(
            url,
            auth=(self.__username, self.__password),
            data=payload
        )
        print(response.text)
 
    def __process_delete_workflow(self, res: dict):
        url = f"http://{self.__host}:{self.__port}/hop/removeWorkflow/?name={res['name_workflow']}&id={res['id_workflow']}"
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
                    if res['type'] == 'pipeline':
                        self.__process_delete_pipeline(res)
                    elif res['type'] == 'workflow':
                        self.__process_delete_workflow(res)
                    already_delete.append(res)
                    continue
 
                if res['status'] != "Finished":
                    continue
                if res['type'] == 'pipeline':
                    self.__process_delete_pipeline(res)
                elif res['type'] == 'workflow':
                    self.__process_delete_workflow(res)
                already_delete.append(res)
            except Exception as e:
                logger.error("Error: {}", str(e))
        return already_delete
 