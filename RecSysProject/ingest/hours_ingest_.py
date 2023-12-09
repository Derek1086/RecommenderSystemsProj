import pandas as pd


# [[INTERNAL]]
# Day List
days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# [[INTERNAL]]
# Time string splitting and reformatting
def process_time(time_str):
    open_time, close_time = time_str.split('-')
    open_hour, open_minute = open_time.split(':')
    close_hour, close_minute = close_time.split(':')
    open_time = f'{int(open_hour):02d}:{int(open_minute):02d}'
    close_time = f'{int(close_hour):02d}:{int(close_minute):02d}'
    return open_time, close_time


# [[INTERNAL]]
# Per row hour processing
def process_hours(row):
    hours_data = row['hours']
    row_data = {}

    if hours_data is not None:
        for day in days_of_week:
            hours = hours_data.get(day)
            if hours:
                open_time, close_time = process_time(hours)
            else:
                open_time, close_time = None, None
            row_data[f'{day.lower()}_open'] = open_time
            row_data[f'{day.lower()}_close'] = close_time
    else:
        for day in days_of_week:
            row_data[f'{day.lower()}_open'] = None
            row_data[f'{day.lower()}_close'] = None
    return pd.Series(row_data)
