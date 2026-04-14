from enum import Enum

class HopMode(Enum): 
    all: str = "All"
    pipeline: str = "Pipeline"
    workflow: str = "Workflow"

class StatusHop(Enum): 
    all: str = "All"
    finished: str = "Finished"
    finishedError: str = "Finished (with errors)"
    running: str = "Running"
    halting: str = "Halting"

class Orders(Enum): 
    desc: bool = "desc"
    asc: bool = "asc"

class OrdersBy(Enum): 
    startDate: str = "startDate"
    duration: str = "durationRaw"

class OptionsMode(Enum): 
    none: str = None
    start: str = "start"
    stop: str = "stop"
    remove: str = "remove"
