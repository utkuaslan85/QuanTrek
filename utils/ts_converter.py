from datetime import datetime
from dateutil import parser
import time

def to_unix_timestamp_seconds(input_time):
    """
    Converts various time representations to a Unix timestamp in seconds.

    Args:
        input_time: Can be a datetime object, a Unix timestamp (int/float),
                    or an ISO 8601 formatted string.

    Returns:
        An integer representing the Unix timestamp in seconds.

    Raises:
        ValueError: If the input_time format is not recognized or cannot be converted.
    """
    if isinstance(input_time, (int, float)):
        # Assume it's already a Unix timestamp (seconds or milliseconds)
        # If it's a large number, assume milliseconds and convert to seconds
        if input_time > 2 * 10**10:  # Arbitrary large number to guess milliseconds
            return int(input_time / 1000)
        return int(input_time)
    elif isinstance(input_time, datetime):
        # datetime object
        # Ensure it's UTC for consistent timestamp
        if input_time.tzinfo is None:
            # Naive datetime objects are treated as local time by .timestamp()
            # It's better to explicitly handle timezones.
            # For this function, we'll assume naive datetimes are UTC
            # if you want consistent UTC timestamps.
            # Otherwise, .timestamp() will give local timestamp.
            # If you want local time, remove the .astimezone(pytz.utc) part
            # and just use int(input_time.timestamp())
            import pytz
            input_time = pytz.utc.localize(input_time) # Assume naive is UTC
        return int(input_time.timestamp())
    elif isinstance(input_time, str):
        try:
            # Try parsing as ISO 8601 or other common date strings
            dt_object = parser.parse(input_time)
            # Ensure it's UTC for consistent timestamp
            if dt_object.tzinfo is None:
                # If parsed string doesn't have timezone, assume UTC
                import pytz
                dt_object = pytz.utc.localize(dt_object)
            return int(dt_object.timestamp())
        except parser.ParserError:
            raise ValueError(f"Could not parse string as a datetime: {input_time}")
    else:
        raise ValueError(f"Unsupported input type: {type(input_time)}. Must be int, float, datetime, or string.")

# --- Examples ---
if __name__ == "__main__":
    # Import pytz for timezone awareness
    import pytz

    # 1. From a datetime object (naive - usually interpreted as local time by .timestamp())
    dt_naive_local = datetime(2023, 10, 26, 10, 30, 0)
    print(f"Naive datetime (local): {dt_naive_local} -> {to_unix_timestamp_seconds(dt_naive_local)}")

    # 2. From a timezone-aware datetime object (UTC)
    dt_aware_utc = datetime(2023, 10, 26, 10, 30, 0, tzinfo=pytz.utc)
    print(f"Aware datetime (UTC): {dt_aware_utc} -> {to_unix_timestamp_seconds(dt_aware_utc)}")

    # 3. From a timezone-aware datetime object (another timezone)
    london_tz = pytz.timezone('Europe/London')
    dt_aware_london = london_tz.localize(datetime(2023, 10, 26, 10, 30, 0))
    print(f"Aware datetime (London): {dt_aware_london} -> {to_unix_timestamp_seconds(dt_aware_london)}")

    # 4. From an ISO 8601 string (UTC 'Z')
    iso_string_utc = "2023-03-15T12:00:00Z"
    print(f"ISO 8601 (UTC): {iso_string_utc} -> {to_unix_timestamp_seconds(iso_string_utc)}")

    # 5. From an ISO 8601 string (with offset)
    iso_string_offset = "2023-03-15T12:00:00+02:00"
    print(f"ISO 8601 (offset): {iso_string_offset} -> {to_unix_timestamp_seconds(iso_string_offset)}")

    # 6. From a simple date string (parser.parse handles this, assumes local time if no TZ info)
    date_string = "2024-01-01"
    print(f"Date string: {date_string} -> {to_unix_timestamp_seconds(date_string)}")

    # 7. From a raw Unix timestamp (integer)
    raw_timestamp_int = 1678886400
    print(f"Raw Unix timestamp (int): {raw_timestamp_int} -> {to_unix_timestamp_seconds(raw_timestamp_int)}")

    # 8. From a raw Unix timestamp (float, usually microseconds/milliseconds)
    raw_timestamp_float = 1678886400.123456
    print(f"Raw Unix timestamp (float): {raw_timestamp_float} -> {to_unix_timestamp_seconds(raw_timestamp_float)}")

    # 9. From a raw Unix timestamp (large, potentially milliseconds)
    raw_timestamp_ms = 1678886400000  # Example: March 15, 2023 12:00:00 PM UTC in milliseconds
    print(f"Raw Unix timestamp (milliseconds): {raw_timestamp_ms} -> {to_unix_timestamp_seconds(raw_timestamp_ms)}")

    # 10. Invalid input
    try:
        to_unix_timestamp_seconds("not a time")
    except ValueError as e:
        print(f"Error handling invalid string: {e}")

    try:
        to_unix_timestamp_seconds([1, 2, 3])
    except ValueError as e:
        print(f"Error handling unsupported type: {e}")