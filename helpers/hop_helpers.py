from datetime import datetime, timezone


def durationParser(start_date: str, end_date: str) -> str: 
    dt_start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%f%z")
    dt_end = datetime.now().astimezone(timezone.utc)
    if end_date:
        dt_end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S.%f%z")

    duration = dt_end - dt_start
    seconds = duration.total_seconds()

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    remaining_sec = seconds % 60
    if hours > 0:
        return f"{hours}h {minutes}m {remaining_sec:.2f}s"
    elif minutes > 0:
        return f"{minutes}m {remaining_sec:.2f}s"
    else: 
        return f"{remaining_sec:.2f}s"

def mapper_pipeline_data(resp: dict, mode: str, ): 
    results = []
    orcestration_list = []
    if mode == "Pipeline": 
        orcestration_list = resp['pipelineStatusList'].copy()
    elif mode == "Workflow": 
        orcestration_list = resp['workflowStatusList'].copy()
    else: 
        orcestration_list = resp['pipelineStatusList'] + resp['workflowStatusList']

    for pipe in orcestration_list: 
        results.append(
            {
                "id": pipe['id'],
                "name": pipe['pipelineName'] if 'pipelineName' in pipe else pipe['workflowName'],
                "status": pipe['statusDescription'],
                "startDate": pipe['executionStartDate'],
                "endDate": pipe['executionEndDate'],
                "duration": durationParser(pipe['executionStartDate'], pipe['executionEndDate']),
                "type": "Pipeline" if 'pipelineName' in pipe else "Workflow",
            }
        )
    return results

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