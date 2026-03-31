from datetime import datetime, timezone
from loguru import logger
import re

def parse_date(item):
    return datetime.strptime(item["startDate"], "%Y-%m-%dT%H:%M:%S.%f%z")

def durationParser(start_date: str, end_date: str, secondsFormat: bool = False) -> str: 
    dt_start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%f%z")
    dt_end = datetime.now().astimezone(timezone.utc)
    if end_date:
        dt_end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S.%f%z")

    duration = dt_end - dt_start
    seconds = duration.total_seconds()

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    remaining_sec = seconds % 60
    if secondsFormat is True: 
        return int(seconds)

    if hours > 0:
        return f"{hours}h {minutes}m {remaining_sec:.2f}s"
    elif minutes > 0:
        return f"{minutes}m {remaining_sec:.2f}s"
    else: 
        return f"{remaining_sec:.2f}s"
    
def uptime_parser(time: int): 
    hours = (time / (1000 * 60 * 60))
    minutes = hours * 60 - (int(hours)*60)
    return f"{int(hours)}h {int(minutes)}m"

def mapper_pipeline_data(resp: dict, mode: str, search_name: str): 
    results = []
    orcestration_list = []
    search_name = None if search_name == "" else search_name
    pattern = rf"{search_name}"

    if mode == "Pipeline": 
        orcestration_list = resp['pipelineStatusList'].copy()
    elif mode == "Workflow": 
        orcestration_list = resp['workflowStatusList'].copy()
    else: 
        orcestration_list = resp['pipelineStatusList'] + resp['workflowStatusList']

    for pipe in orcestration_list: 
        mapped = {
            "id": pipe['id'],
            "name": pipe['pipelineName'] if 'pipelineName' in pipe else pipe['workflowName'],
            "status": pipe['statusDescription'],
            "startDate": pipe['executionStartDate'],
            "endDate": pipe['executionEndDate'],
            "duration": durationParser(pipe['executionStartDate'], pipe['executionEndDate']),
            "durationRaw": durationParser(pipe['executionStartDate'], pipe['executionEndDate'], True),
            "type": "Pipeline" if 'pipelineName' in pipe else "Workflow",
        }
        if search_name is not None: 
            match = re.search(pattern, mapped['name'], re.IGNORECASE)
            if not match: 
                continue
            results.append(mapped)
            continue

        results.append(mapped)
    return results

def filter_hop(status: str, results: list): 
    if status == "All": 
        return results
    new_results = []
    for result in results: 
        if status == result['status']: 
            new_results.append(result)
    return new_results

def mapper_pipeline_detail(resp: dict): 
    return {
        "id": resp['id'],
        "name": resp['pipelineName'],
        "status": resp['statusDescription'],
        "startDate": resp['executionStartDate'],
        "endDate": resp['executionEndDate'],
        "duration": durationParser(resp['executionStartDate'], resp['executionEndDate']),
        "transformStatusList": resp['transformStatusList'],
        "totalRead": resp['result']['nrLinesRead'],
        "totalWritten": resp['result']['nrLinesWritten'],
        "totalError": resp['result']['nrErrors'],
        "totalTransform": len(resp['transformStatusList']),
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type": "Pipeline"
    }