#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
import datetime
import time
import calendar


def to_ts(dt):
    if isinstance(dt, datetime.datetime):
        return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())
    if isinstance(dt, datetime.date):
        d = datetime.datetime.combine(dt, datetime.datetime.min.time())
        return int((d - datetime.datetime(1970, 1, 1)).total_seconds())
    return int(dt)


def from_ts(ts):
    if isinstance(ts, datetime.datetime):
        return ts
    if isinstance(ts, datetime.date):
        return datetime.datetime.combine(ts, datetime.datetime.min.time())
    return datetime.datetime.utcfromtimestamp(ts)


def trim_timetuple(time_tuple, trim):
    if trim == "minute":
        return time.struct_time((time_tuple.tm_year,
                                 time_tuple.tm_mon,
                                 time_tuple.tm_mday,
                                 time_tuple.tm_hour,
                                 time_tuple.tm_min,
                                 0,  # time_tuple.tm_sec,
                                 time_tuple.tm_wday,
                                 time_tuple.tm_yday,
                                 time_tuple.tm_isdst))
    elif trim == "hour":
        return time.struct_time((time_tuple.tm_year,
                                 time_tuple.tm_mon,
                                 time_tuple.tm_mday,
                                 time_tuple.tm_hour,
                                 0,  # time_tuple.tm_min,
                                 0,  # time_tuple.tm_sec,
                                 time_tuple.tm_wday,
                                 time_tuple.tm_yday,
                                 time_tuple.tm_isdst))
    elif trim == "day":
        return time.struct_time((time_tuple.tm_year,
                                 time_tuple.tm_mon,
                                 time_tuple.tm_mday,
                                 0,  # time_tuple.tm_hour,
                                 0,  # time_tuple.tm_min,
                                 0,  # time_tuple.tm_sec,
                                 time_tuple.tm_wday,
                                 time_tuple.tm_yday,
                                 time_tuple.tm_isdst))
    elif trim == "week":
        day_in_week = time_tuple.tm_wday
        tt = trim_timetuple(time_tuple, "day")
        ts = int(calendar.timegm(tt))
        ts = ts - (day_in_week * 24 * 60 * 60)
        return time.gmtime(ts)
    elif trim == "month":
        day_in_month = time_tuple.tm_mday - 1
        tt = trim_timetuple(time_tuple, "day")
        ts = int(calendar.timegm(tt))
        ts = ts - (day_in_month * 24 * 60 * 60)
        return time.gmtime(ts)
    else:
        raise ValueError("invalid trim parameter")


def ts_hourly_left(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "hour")
    return int(calendar.timegm(n))


def ts_hourly_right(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "hour")
    ts = int(calendar.timegm(n)) + 60 * 60 - 1
    return ts


def ts_daily_left(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "day")
    return int(calendar.timegm(n))


def ts_daily_right(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "day")
    ts = int(calendar.timegm(n)) + 24 * 60 * 60 - 1
    return ts


def ts_weekly_left(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "week")
    return int(calendar.timegm(n))


def ts_weekly_right(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "week")
    ts = int(calendar.timegm(n)) + 7 * 24 * 60 * 60 - 1
    return ts


def ts_monthly_left(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "month")
    return int(calendar.timegm(n))


def ts_monthly_right(ts):
    time_tuple = time.gmtime(ts)
    n = trim_timetuple(time_tuple, "month")
    month = n.tm_mon
    days = [None, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    year = n.tm_year
    if year % 4 == 0:
        days[2] = 29
    ts = int(calendar.timegm(n)) + days[month] * 24 * 60 * 60 - 1
    return ts
