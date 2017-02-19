#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import os


from stss.storage import TSDB


class DatabaseTest(unittest.TestCase):
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

    def test_simple(self):
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        cassandra_port = os.getenv('CASSANDRA_PORT', 9042)
        db = TSDB(STORAGE="cassandra",
                  CASSANDRA_HOST=cassandra_host,
                  CASSANDRA_PORT=cassandra_port)
        db.storage._dropTable()
        db.storage._createTable()

        d1 = [(i * 600, 6.5) for i in range(500)]
        d2 = [(i * 600, 25.5) for i in range(500)]
        d3 = [(i * 600, 10.5) for i in range(500)]

        data = [{"key": "sensor1.ph",
                 "data": d1},
                {"key": "sensor1.temp",
                 "data": d2}]
        db.insert_bulk(data)
        db.insert("sensor1.act", d3)

        s = db.stats("sensor1.act")
        self.assertEqual(s["ts_min"], 0)
        self.assertEqual(s["ts_max"], 499 * 600)
        self.assertEqual(s["count"], 500)
        self.assertEqual(s["key"], "sensor1.act")

        s = db.stats_bulk(["sensor1.ph", "sensor1.temp"])
        self.assertEqual(len(s), 2)
        self.assertEqual(s[0]["ts_min"], 0)
        self.assertEqual(s[0]["ts_max"], 499 * 600)
        self.assertEqual(s[0]["count"], 500)
        self.assertEqual(s[0]["key"], "sensor1.ph")
        self.assertEqual(s[1]["ts_min"], 0)
        self.assertEqual(s[1]["ts_max"], 499 * 600)
        self.assertEqual(s[1]["count"], 500)
        self.assertEqual(s[1]["key"], "sensor1.temp")

        r = db.query("sensor1.act", 0, 500*600)
        a = list(r.all())
        d = list(r.aggregation("daily", "mean"))
        self.assertEqual(len(a), 500)
        self.assertEqual(len(d), 4)
        for ts, v in d:
            self.assertAlmostEqual(v, 10.5, 4)

        r = db.query("sensor1.ph", 0, 500*600)
        a = list(r.all())
        d = list(r.aggregation("daily", "mean"))
        self.assertEqual(len(a), 500)
        self.assertEqual(len(d), 4)
        for ts, v in d:
            self.assertAlmostEqual(v, 6.5, 4)

        r = db.query("sensor1.temp", 0, 500*600)
        a = list(r.all())
        d = list(r.aggregation("daily", "mean"))
        self.assertEqual(len(a), 500)
        self.assertEqual(len(d), 4)
        for ts, v in d:
            self.assertAlmostEqual(v, 25.5, 4)

        # todo bulk query

    def test_cassandra_rewrite(self):
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        cassandra_port = os.getenv('CASSANDRA_PORT', 9042)
        db = TSDB(STORAGE="cassandra",
                  BUCKET_TYPE="daily",
                  CASSANDRA_HOST=cassandra_host,
                  CASSANDRA_PORT=cassandra_port)
        db.storage._dropTable()
        db.storage._createTable()
        for i in range(0, 300):
            db._insert("cass", [(i * 10 * 60, 1.1)])

        res = db.storage.query("cass", 0, 4 * 24 * 60 * 60)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(res[0]), 144)
        self.assertEqual(len(res[1]), 144)
        self.assertEqual(len(res[2]), 12)

        res = db._query("cass", 0, 4 * 24 * 60 * 60)
        self.assertEqual(len(res), 300)
        self.assertEqual(res[144][0], 24 * 60 * 60)
        self.assertAlmostEqual(res[144][1], 1.1, 4)

        for i in range(0, 300):
            db._insert("cass", [(i * 10 * 60, 2.2)])

        res = db.storage.query("cass", 0, 4 * 24 * 60 * 60)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(res[0]), 144)
        self.assertEqual(len(res[1]), 144)
        self.assertEqual(len(res[2]), 12)

        res = db._query("cass", 0, 4 * 24 * 60 * 60)
        self.assertEqual(len(res), 300)
        self.assertEqual(res[144][0], 24 * 60 * 60)
        self.assertAlmostEqual(res[144][1], 1.1, 4)

        for i in range(0, 300):
            db._insert("cass", [(i * 10 * 60 + 1, 2.2)])

        res = db.storage.query("cass", 0, 4 * 24 * 60 * 60 + 1)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(res[0]), 288)
        self.assertEqual(len(res[1]), 288)
        self.assertEqual(len(res[2]), 24)

        res = db._query("cass", 0, 4 * 24 * 60 * 60 + 1)
        self.assertEqual(len(res), 600)
        self.assertEqual(res[288][0], 24 * 60 * 60)
        self.assertAlmostEqual(res[288][1], 1.1, 4)
        self.assertEqual(res[289][0], 24 * 60 * 60 + 1)
        self.assertAlmostEqual(res[289][1], 2.2, 4)

    def test_invalidmetricname(self):
        with self.assertRaises(ValueError):
            d = TSDB()
            d._insert("hüü", [(1, 1.1)])

    def test_merge(self):
        d = TSDB(BUCKET_TYPE="dynamic", BUCKET_DYNAMIC_TARGET=2, BUCKET_DYNAMIC_MAX=2)
        d._insert("merge", [(1, 2.0), (2, 3.0), (5, 6.0), (6, 7.0),
                            (9, 10.0), (0, 1.0)])
        res = d._query("merge", 0, 10)
        self.assertEqual(len(res), 6)
        d._insert("merge", [(3, 4.0), (4, 5.0), (7, 8.0), (8, 9.0)])
        buckets = d.storage.query("merge", 0, 10)
        self.assertEqual(len(buckets), 5)
        for b in buckets:
            self.assertEqual(len(b), 2)

        res = d._query("merge", 0, 10)
        self.assertEqual(len(res), 10)
        for ts, v in res.all():
            self.assertAlmostEqual(float(ts + 1.0), v)

    def test_dynamic(self):
        d = TSDB(BUCKET_TYPE="dynamic", BUCKET_DYNAMIC_TARGET=3, BUCKET_DYNAMIC_MAX=3)
        d._insert("hi", [(1, 1.1), (2, 2.2)])
        d._insert("hi", [(4, 4.4)])
        i = d.storage.last("hi")
        self.assertEqual(len(i), 3)
        self.assertEqual(i[0][0], 1)
        self.assertEqual(i[1][0], 2)
        self.assertEqual(i[2][0], 4)

        d._insert("hi", [(3, 3.3)])
        buckets = d.storage.query("hi", 0, 10)
        self.assertEqual(len(buckets), 2)
        i = buckets[0]
        self.assertEqual(len(i), 3)
        self.assertEqual(i[0][0], 1)
        self.assertEqual(i[1][0], 2)
        self.assertEqual(i[2][0], 3)

        i2 = buckets[1]
        self.assertEqual(len(i2), 1)
        self.assertEqual(i2[0][0], 4)

    def test_hourly(self):
        d = TSDB(BUCKET_TYPE="hourly")
        for i in range(0, 70):
            d._insert("his", [(i * 60, 1.1)])

        buckets = d.storage.query("his", 0, 70*60)
        self.assertEqual(len(buckets), 2)
        i = buckets[0]
        self.assertEqual(len(i), 60)
        self.assertEqual(i[0][0], 0)
        self.assertEqual(i[59][0], 59*60)

        i2 = buckets[1]
        self.assertEqual(len(i2), 10)
        self.assertEqual(i2[0][0], 60*60)
        self.assertEqual(i2[9][0], 69*60)

    def test_daily(self):
        d = TSDB(BUCKET_TYPE="daily")
        for i in range(0, 50):
            d._insert("daily", [(i * 60 * 30, 1.1)])

        buckets = d.storage.query("daily", 0, 50 * 60 * 30)
        self.assertEqual(len(buckets), 2)
        i = buckets[0]
        self.assertEqual(len(i), 48)
        self.assertEqual(i[0][0], 0)
        self.assertEqual(i[47][0], 47*60*30)

        i2 = buckets[1]
        self.assertEqual(len(i2), 2)
        self.assertEqual(i2[0][0], 48 * 30 * 60)
        self.assertEqual(i2[1][0], 49 * 30 * 60)

    def test_weekly(self):
        d = TSDB(BUCKET_TYPE="weekly")
        for i in range(0, 20):
            d._insert("weekly", [(i * 24 * 60 * 60, 1.1)])

        buckets = d.storage.query("weekly", 0, 20 * 24 * 60 * 60)
        self.assertEqual(len(buckets), 4)
        i = buckets[0]
        self.assertEqual(len(i), 4)
        self.assertEqual(i[0][0], 0)
        self.assertEqual(i[3][0], 3 * 24 * 60 * 60)

        i2 = buckets[1]
        self.assertEqual(len(i2), 7)
        self.assertEqual(i2[0][0], 4 * 24 * 60 * 60)
        self.assertEqual(i2[6][0], 10 * 24 * 60 * 60)

    def test_monthly(self):
        d = TSDB(BUCKET_TYPE="monthly")
        for i in range(0, 40):
            d._insert("monthly", [(i * 24 * 60 * 60, 1.1)])

        buckets = d.storage.query("monthly", 0, 40 * 24 * 60 * 60)
        self.assertEqual(len(buckets), 2)
        i = buckets[0]
        self.assertEqual(len(i), 31)
        self.assertEqual(i[0][0], 0)
        self.assertEqual(i[30][0], 30 * 24 * 60 * 60)

        i2 = buckets[1]
        self.assertEqual(len(i2), 9)
        self.assertEqual(i2[0][0], 31 * 24 * 60 * 60)
        self.assertEqual(i2[8][0], 39 * 24 * 60 * 60)

    def test_largedataset(self):
        # Generate
        d = []
        for i in range(50000):
            d.append((i, i * 2.5))

        s = []
        while len(d) > 0:
            count = random.randint(3, 30)
            s.append([])
            for _ in range(count):
                if len(d) < 1:
                    break
                el = d.pop(0)
                s[-1].append(el)

        # Make some holes
        s.insert(200, s.pop(100))
        s.insert(200, s.pop(100))
        s.insert(200, s.pop(100))
        s.insert(1000, s.pop(1100))
        s.insert(1200, s.pop(1300))
        s.insert(1400, s.pop(1400))

        # Strange Future Hole
        s.insert(2000, s.pop(1800))

        # Insert
        d = TSDB(BUCKET_TYPE="dynamic", BUCKET_DYNAMIC_TARGET=100)
        for p in s:
            d._insert("large", p)

        buckets = d.storage.query("large", 0, 50000)
        self.assertGreater(len(buckets), 450)
        self.assertLess(len(buckets), 550)

        res = d._query("large", 1, 50000)
        self.assertEqual(len(res), 49999)
        res = d._query("large", 0, 49999)
        self.assertEqual(len(res), 50000)
