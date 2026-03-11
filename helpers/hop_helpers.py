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
