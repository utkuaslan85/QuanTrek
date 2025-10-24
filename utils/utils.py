from datetime import datetime
from zoneinfo import ZoneInfo
from src.config import settings

def get_now():
    """Return current datetime in the configured timezone."""
    return datetime.now(ZoneInfo(settings.TIMEZONE))
