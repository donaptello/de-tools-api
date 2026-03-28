import requests
import json
import traceback
 
from config.base import settings
from bs4 import BeautifulSoup
from loguru import logger
from helpers.hop_helpers import (
    mapper_pipeline_data,
    mapper_pipeline_detail,
    uptime_parser
)
 
class HopService:
 
    def __init__(self, mode: str = None):
        self.__host: str = settings.APACHE_HOP_HOST
        self.__port: str = settings.APACHE_HOP_PORT
        self.__username: str = settings.APACHE_HOP_USER
        self.__password: str = settings.APACHE_HOP_PASS
        self.__mode: str = mode
        self.__test: bool = settings.TESTING_API

    def __api_hop(
        self, 
        method: str, 
        route: str, 
        param_id: str = None, 
        param_name: str = None
    ): 
        if self.__test: 
            if route == "status": 
                with open('resources/hop-status-response.json', 'r') as file: 
                    resp = json.load(file)
            else: 
                with open('resources/hop-pipeline-response.json', 'r') as file: 
                    resp = json.load(file)
            return resp
        uri = f"http://{self.__host}:{self.__port}/{route}?json=y"
        if param_id is not None: 
            uri = f"http://{self.__host}:{self.__port}/{route}?id={param_id}&json=y"
        if param_id is not None and param_name is not None: 
            uri = f"{uri}&name={param_name}"
        
        resp = requests.request(
            method,
            uri,
            auth=(self.__username, self.__password)
        )
        if resp.status_code == 200:
            return resp.json()
        return {'error': resp.text}
    
    def get_status(self):
        resp = self.__api_hop(
            method="GET",
            route="/hop/status" if not self.__test else "status"
        )
        if 'error' in resp: 
            return resp
        return {
            "statusHop": resp.get("statusDescription"),
            "pipelineStatus": {
                "total": len(resp.get("pipelineStatusList")),
                "totalRunning": len([
                    pipeline
                    for pipeline in resp['pipelineStatusList']
                    if pipeline['running'] is True
                ]),
                "totalFinished": len([
                    pipeline
                    for pipeline in resp['pipelineStatusList']
                    if pipeline['finished'] is True
                ]),
                "totalError": len([
                    pipeline
                    for pipeline in resp['pipelineStatusList']
                    if pipeline['stopped'] is True 
                        or pipeline['waiting'] is True 
                        or pipeline['paused'] is True
                ]),
            },
            "workflowStatus": {
                "total": len(resp.get("workflowStatusList")),
                "totalRunning": len([
                    workflow
                    for workflow in resp['workflowStatusList']
                    if workflow['running'] is True
                ]),
                "totalFinished": len([
                    workflow
                    for workflow in resp['workflowStatusList']
                    if workflow['finished'] is True
                ]),
                "totalError": len([
                    workflow
                    for workflow in resp['workflowStatusList']
                    if workflow['stopped'] is True 
                        or workflow['waiting'] is True
                ]),
            },
            "memoryFree": resp['memoryFree'] / 1073741824,
            "memoryTotal": resp['memoryTotal'] / 1073741824,
            "memoryUsed": (resp['memoryTotal'] / 1073741824) - (resp['memoryFree'] / 1073741824),
            "cpuCores": resp["cpuCores"],
            "cpuProcessTime": resp["cpuProcessTime"],
            "uptime": uptime_parser(resp["uptime"]),
            "threadCount": resp["threadCount"],
            "loadAvg": resp["loadAvg"],
        }
    
    def get_pipeline_v2(
        self, 
        params_id: str = None, 
        params_name: str = None,
        search_name: str = None
    ): 
        if params_id is None: 
            resp = self.__api_hop(
                method="GET",
                route="/hop/status" if not self.__test else "status"
            )
            results = mapper_pipeline_data(resp, self.__mode, search_name)
            return results
        else: 
            resp = self.__api_hop(
                method="GET",
                route="/hop/pipelineStatus" if not self.__test else "pipeline",
                param_id=params_id,
                param_name=params_name
            )
            results = mapper_pipeline_detail(resp)
            results['name'] = params_name
            return [results]

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
            return results_all_mapped, "Pipeline"
        elif self.__mode == "Workflow":
            return results_all_mapped, "Workflow"
        return results_all_mapped, "All"
   
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

    def __process_delete_hit_api_hop(
        self, 
        with_error: bool, 
        results_data: list,
        already_delete: list
    ): 
        for res in results_data:
            try:
                if with_error and res['status'] in ["Finished (with errors)", "Stopped", "Stopped (with errors)"]:
                    if res['type'] == 'Pipeline' and self.__mode in ["Pipeline", "All"]:
                        self.__process_delete_pipeline(res)
                    elif res['type'] == 'Workflow' and self.__mode in ["Workflow", "All"]:
                        self.__process_delete_workflow(res)
                    already_delete.append(res)
                    continue

                if res['status'] != "Finished":
                    continue
                if res['type'] == 'Pipeline' and self.__mode in ["Pipeline", "All"]:
                    self.__process_delete_pipeline(res)
                elif res['type'] == 'Workflow' and self.__mode in ["Workflow", "All"]:
                    self.__process_delete_workflow(res)
                already_delete.append(res)
            except Exception as e:
                logger.error("Error: {}", str(e))
                traceback.print_exc()
   
    def delete_pipeline(self, with_error: bool):
        results_data = self.get_pipeline_v2()
        already_delete = []
        
        if self.__mode == "Pipeline": 
            self.__process_delete_hit_api_hop(
                with_error, 
                results_data,
                already_delete
            )

        elif self.__mode == "Workflow": 
            self.__process_delete_hit_api_hop(
                with_error, 
                results_data,
                already_delete
            )
        else: 
            self.__process_delete_hit_api_hop(
                with_error,
                results_data,
                already_delete
            )
        return already_delete
 
    def delete_pipeline_v2(self, with_error: bool): 
        pass