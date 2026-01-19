"""
常用的时间函数工具
"""
from datetime import datetime

from vnag.local import LocalTool


def current_date() -> str:
    """Get the current date string in YYYY-MM-DD format"""
    return datetime.now().strftime("%Y-%m-%d")


def current_time() -> str:
    """Get the current time string in HH:MM:SS format"""
    return datetime.now().strftime("%H:%M:%S")


def current_datetime() -> str:
    """Get the current date and time string in YYYY-MM-DD HH:MM:SS format"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def day_of_week() -> str:
    """Get the day of the week in Chinese format (for example: Monday)"""
    weekday_map = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    weekday = datetime.now().weekday()
    return weekday_map[weekday]


#Registration tool
current_date_tool: LocalTool = LocalTool(current_date)

current_time_tool: LocalTool = LocalTool(current_time)

current_datetime_tool: LocalTool = LocalTool(current_datetime)

day_of_week_tool: LocalTool = LocalTool(day_of_week)
