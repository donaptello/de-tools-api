from enum import Enum

class HopMode(Enum): 
    all: str = "All"
    pipeline: str = "Pipeline"
    workflow: str = "Workflow"