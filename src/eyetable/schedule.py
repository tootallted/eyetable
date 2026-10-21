"""When a group of sources should load: a small, readable schedule format.

Two kinds, both limited to chosen weekdays and expressed in a named timezone:

    {"type": "daily",  "times": ["06:00"], "days": [0, 1, 2, 3, 4], "timezone": "America/Los_Angeles"}
    {"type": "hourly", "every": 1, "start": "08:00", "end": "17:00", "days": [0, 1, 2, 3, 4],
     "timezone": "America/Los_Angeles"}

Days are 0 = Monday ... 6 = Sunday. "hourly" runs at start, start + every
hours, ... up to and including end (08:00-17:00 hourly = 08:00, 09:00, ...,
17:00). Both reduce to a set of local times on chosen days, which is all the
next-run calculation needs.
"""

import datetime as dt
import re
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
DAY_SHORT = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
EVERY_HOURS = (1, 2, 3, 4, 6, 8, 12)
_TIME = re.compile(r"([01]\d|2[0-3]):([0-5]\d)")


class ScheduleError(ValueError):
    pass


def _parse_time(value: Any, what: str) -> dt.time:
    if not isinstance(value, str) or not _TIME.fullmatch(value):
        raise ScheduleError(f"{what} must be a time like 06:00, got {value!r}")
    hour, minute = map(int, value.split(":"))
    return dt.time(hour, minute)


def normalize(schedule: dict[str, Any]) -> dict[str, Any]:
    """Validate a schedule and return it in canonical form (raises ScheduleError)."""
    if not isinstance(schedule, dict):
        raise ScheduleError("a schedule must be an object")
    kind = schedule.get("type")
    tz = schedule.get("timezone") or "UTC"
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise ScheduleError(f"unknown timezone {tz!r}") from None
    days = schedule.get("days", list(range(7)))
    if not isinstance(days, list) or not days or any(d not in range(7) for d in days):
        raise ScheduleError("pick at least one day (0 = Monday ... 6 = Sunday)")
    days = sorted(set(days))

    if kind == "daily":
        times = schedule.get("times")
        if not isinstance(times, list) or not times:
            raise ScheduleError("a daily schedule needs at least one time")
        if len(times) > 24:
            raise ScheduleError("a daily schedule can have at most 24 times")
        parsed = sorted({_parse_time(t, "each time") for t in times})
        return {"type": "daily", "times": [t.strftime("%H:%M") for t in parsed], "days": days, "timezone": tz}
    if kind == "hourly":
        every = schedule.get("every", 1)
        if every not in EVERY_HOURS:
            raise ScheduleError(f"every must be one of {', '.join(map(str, EVERY_HOURS))} hours")
        start = _parse_time(schedule.get("start", "00:00"), "start")
        end = _parse_time(schedule.get("end", "23:00"), "end")
        if end < start:
            raise ScheduleError("the end time must not be before the start time")
        return {"type": "hourly", "every": every, "start": start.strftime("%H:%M"), "end": end.strftime("%H:%M"),
                "days": days, "timezone": tz}
    raise ScheduleError('schedule type must be "daily" or "hourly"')


def times_of_day(schedule: dict[str, Any]) -> list[dt.time]:
    if schedule["type"] == "daily":
        return [_parse_time(t, "time") for t in schedule["times"]]
    start = _parse_time(schedule["start"], "start")
    end = _parse_time(schedule["end"], "end")
    out, minutes = [], start.hour * 60 + start.minute
    while minutes <= end.hour * 60 + end.minute:
        out.append(dt.time(minutes // 60, minutes % 60))
        minutes += schedule["every"] * 60
    return out


def next_runs(schedule: dict[str, Any], after: dt.datetime, count: int = 1) -> list[dt.datetime]:
    """The next `count` run times strictly after `after` (aware datetimes, UTC)."""
    schedule = normalize(schedule)
    tz = ZoneInfo(schedule["timezone"])
    times = times_of_day(schedule)
    local_after = after.astimezone(tz)
    found: list[dt.datetime] = []
    day = local_after.date()
    for _ in range(366):
        if day.weekday() in schedule["days"]:
            for t in times:
                candidate = dt.datetime.combine(day, t, tzinfo=tz)
                # A wall time skipped by a DST jump doesn't exist; fold=0 gives the
                # pre-jump offset, which lands it an hour later in real time.
                utc = candidate.astimezone(dt.timezone.utc)
                if utc > after.astimezone(dt.timezone.utc) and (not found or utc > found[-1]):
                    found.append(utc)
                    if len(found) == count:
                        return found
        day += dt.timedelta(days=1)
    return found


def _clock(t: dt.time) -> str:
    hour = t.hour % 12 or 12
    return f"{hour}:{t.minute:02d} {'AM' if t.hour < 12 else 'PM'}"


def _days_text(days: list[int]) -> str:
    if days == list(range(7)):
        return "every day"
    if days == list(range(5)):
        return "Monday to Friday"
    if days == [5, 6]:
        return "on weekends"
    if len(days) == 1:
        return f"on {DAY_NAMES[days[0]]}s"
    # runs of consecutive days read better as ranges: Mon–Wed, Fri
    parts, run = [], [days[0]]
    for d in days[1:]:
        if d == run[-1] + 1:
            run.append(d)
        else:
            parts.append(run)
            run = [d]
    parts.append(run)
    return "on " + ", ".join(DAY_SHORT[r[0]] if len(r) == 1 else f"{DAY_SHORT[r[0]]}–{DAY_SHORT[r[-1]]}" for r in parts)


def describe(schedule: dict[str, Any] | None) -> str:
    if not schedule:
        return "Not scheduled"
    s = normalize(schedule)
    days = _days_text(s["days"])
    if s["type"] == "daily":
        times = [_clock(_parse_time(t, "time")) for t in s["times"]]
        when = times[0] if len(times) == 1 else ", ".join(times[:-1]) + " and " + times[-1]
        return f"Daily at {when}, {days}" if days != "every day" else f"Every day at {when}"
    every = "Every hour" if s["every"] == 1 else f"Every {s['every']} hours"
    start, end = _clock(_parse_time(s["start"], "start")), _clock(_parse_time(s["end"], "end"))
    window = "all day" if s["start"] == "00:00" and s["end"] == "23:00" and s["every"] == 1 else f"from {start} to {end}"
    return f"{every} {window}, {days}"
