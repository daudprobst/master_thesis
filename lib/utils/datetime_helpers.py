from datetime import datetime, timedelta

def unix_ms_to_date(unix_ms: int) -> datetime:
    return datetime.fromtimestamp(float(unix_ms/1000))

def round_to_hour(t: datetime) -> datetime:
    """Returns the datetime rounded up/down to the hour"""
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))

def round_to_hour_slots(t: datetime, hours_in_slot=6) -> datetime:
    """Returns the datetime grouped into slots of size hours_in_slot
    (e.g. for a slot size of 6h: 00:00:00, 06:00:00, 12:00:00, 18:00:00, 24:00:00"""
    rounded_by_hour = round_to_hour(t)
    return rounded_by_hour.replace(hour=((rounded_by_hour.hour // hours_in_slot) * hours_in_slot))
