#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import datetime
import time


from stss.storage.helper import to_ts, from_ts


class HelperTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        pass

    def test_tsconvertion(self):
        d = datetime.date(2000, 1, 1)
        dt = datetime.datetime(2000, 1, 1)
        t1 = to_ts(d)
        t2 = to_ts(dt)
        t3 = to_ts(946684800)
        self.assertEqual(t1, t2)
        self.assertEqual(t2, t3)

        d1 = from_ts(946684800)
        d2 = from_ts(d)
        d3 = from_ts(dt)
        self.assertEqual(d1, d2)
        self.assertEqual(d2, d3)

    def test_trim(self):
        from stss.storage.helper import trim_timetuple
        t = time.time()
        time_tuple = time.gmtime(t)
        n = trim_timetuple(time_tuple, "minute")
        self.assertEqual(n.tm_sec, 0)

        with self.assertRaises(ValueError):
            n = trim_timetuple(time_tuple, "seco")

    def test_findleftsplits(self):
        # Left hourly
        from stss.storage.helper import ts_hourly_left
        t = time.time()
        t = ts_hourly_left(t)
        h = datetime.datetime.utcnow().replace(minute=0, second=0,
                                               microsecond=0)
        h = to_ts(h)

        # Left daily
        from stss.storage.helper import ts_daily_left
        t = time.time()
        t = ts_daily_left(t)
        h = datetime.datetime.utcnow().replace(hour=0,
                                               minute=0, second=0,
                                               microsecond=0)
        h = to_ts(h)

        # Left weekly
        from stss.storage.helper import ts_weekly_left
        t = 1470388856.652508
        t = ts_weekly_left(t)
        h = datetime.datetime(2016, 8, 1, 0, 0, 0)
        h = to_ts(h)

        # Left monthly
        from stss.storage.helper import ts_monthly_left
        t = time.time()
        t = ts_monthly_left(t)
        h = datetime.datetime.utcnow().replace(day=1,
                                               hour=0,
                                               minute=0, second=0,
                                               microsecond=0)
        h = to_ts(h)

    def test_findrightsplits(self):
        # right hourly
        from stss.storage.helper import ts_hourly_right
        t = time.time()
        t = ts_hourly_right(t)
        h = datetime.datetime.utcnow().replace(minute=59, second=59,
                                               microsecond=0)
        h = to_ts(h)

        # right daily
        from stss.storage.helper import ts_daily_right
        t = time.time()
        t = ts_daily_right(t)
        h = datetime.datetime.utcnow().replace(hour=23,
                                               minute=59, second=59,
                                               microsecond=0)
        h = to_ts(h)

        # Left weekly
        from stss.storage.helper import ts_weekly_right
        t = 1470388856.652508
        t = ts_weekly_right(t)
        h = datetime.datetime(2016, 8, 7, 23, 59, 59)
        h = to_ts(h)

        # Left monthly
        from stss.storage.helper import ts_monthly_right
        t = 1470388856.652508
        t = ts_monthly_right(t)
        h = datetime.datetime(2016, 8, 31, 23, 59, 59)
        h = to_ts(h)
