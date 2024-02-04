import pytz
import datetime

def get_current_timestamp():
    warsaw_tz = pytz.timezone('Europe/Warsaw') 
    timestamp = datetime.datetime.now(warsaw_tz).strftime('%Y-%m-%d_%H:%M:%S')
    return timestamp