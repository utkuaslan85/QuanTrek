from datetime import datetime
from zoneinfo import ZoneInfo

def get_now():
    """Return timestamp."""
    return datetime.now().timestamp()

def ts_to_iso(ts, tz: str ='utc'):
    # for turkish time set tz to 'tr'
    if tz == 'utc':
        return datetime.fromtimestamp(ts, ZoneInfo("UTC")).isoformat()
    if tz == 'tr':
        return datetime.fromtimestamp(ts, ZoneInfo("Europe/Istanbul")).isoformat()
    
def iso_to_ts(iso):
    #convert iso format to timestamp
        return datetime.fromisoformat(iso).timestamp()

def normalize_timestamp(timestamp):
    """Convert milliseconds to seconds if needed"""
    if len(str(int(timestamp))) > 10:  # Milliseconds have 13 digits, seconds have 10
        return timestamp / 1000
    return timestamp
