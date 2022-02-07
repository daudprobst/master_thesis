from datetime import datetime, timedelta, time, timezone
from typing import Tuple
import pytz


def unix_ms_to_ger_date(unix_ms: int) -> datetime:
    utc_timestamp = datetime.fromtimestamp(float(unix_ms / 1000.0), tz=timezone.utc)
    ger_tz = pytz.timezone("Europe/Berlin")
    ger_timestamp = utc_timestamp.astimezone(ger_tz)
    return ger_timestamp


def round_to_hour(t: datetime) -> datetime:
    """Returns the datetime rounded down to the hour"""
    return t.replace(second=0, microsecond=0, minute=0, hour=t.hour)


def round_to_hour_slots(t: datetime, hours_in_slot=6) -> datetime:
    """Returns the datetime grouped into slots of size hours_in_slot
    (e.g. for a slot size of 6h: 00:00:00, 06:00:00, 12:00:00, 18:00:00, 24:00:00"""
    rounded_by_hour = round_to_hour(t)
    return rounded_by_hour.replace(
        hour=((rounded_by_hour.hour // hours_in_slot) * hours_in_slot)
    )


def day_wrapping_datetimes(day: datetime) -> Tuple[datetime, datetime]:
    """
    Returns the datetimes for the first second of the day and the first second of the next day
    (we need first second of next day not last second of this day because enddate is exclusive for twitter api)
    :param day: day for which we want the first and last second
    :return: Tuple containing datetimes for first and last second of day
    """

    return (
        datetime.combine(day.date(), time.min, tzinfo=day.tzinfo),
        datetime.combine((day + timedelta(days=1)).date(), time.min, tzinfo=day.tzinfo),
    )
