#!/usr/bin/python
# coding: utf8

import unittest
import logging
import os


from stss.storage.models import Bucket, BucketType
from stss.storage.backend import FileStorage, RedisStorage, DynamoStorage
from stss.errors import NotFoundError


class StorageTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        pass

    def test_dynamostore(self):
        storage = DynamoStorage(table_name="testtable", local_dynamo=True)
        storage._dropTable()
        storage._createTable()
        self.assertTrue(storage)

        with self.assertRaises(NotFoundError):
            storage.get(key="test.ph", range_key=1000)

        with self.assertRaises(NotFoundError):
            storage.last(key="test.ph")

        i = Bucket.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))

        s = storage.range(key="test.ph")
        self.assertEqual(s["ts_min"], 1000)
        self.assertEqual(s["ts_max"], 2000)

    def test_redisstore(self):
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', 6379)
        storage = RedisStorage(host=redis_host, port=redis_port, db=0, expire=5)
        self.assertTrue(storage)

        with self.assertRaises(NotFoundError):
            storage.get(key="test.ph", range_key=1000)

        with self.assertRaises(NotFoundError):
            storage.last(key="test.ph")

        i = Bucket.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))

        s = storage.range(key="test.ph")
        self.assertEqual(s["ts_min"], 1000)
        self.assertEqual(s["ts_max"], 2000)

    def test_memorystore(self):
        test_path = os.path.dirname(os.path.realpath(__file__))
        testdb_dir = os.path.join(test_path, "testdb")
        if not os.path.exists(testdb_dir):
            os.makedirs(testdb_dir)
        for the_file in os.listdir(testdb_dir):
            file_path = os.path.join(testdb_dir, the_file)
            if os.path.isfile(file_path):
                os.unlink(file_path)

        storage = FileStorage(testdb_dir)
        self.assertTrue(storage)

        with self.assertRaises(NotFoundError):
            storage.get(key="test.ph", range_key=1000)

        with self.assertRaises(NotFoundError):
            storage.last(key="test.ph")

        i = Bucket.new("test.ph", [(1000, 1.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(2000, 4.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(1100, 2.0)])
        storage.insert(i)
        i = Bucket.new("test.ph", [(1200, 3.0)])
        storage.insert(i)

        d = storage.get(key="test.ph", range_key=1000)
        self.assertEqual(d[0], (1000, 1.0))
        d = storage.get(key="test.ph", range_key=1100)
        self.assertEqual(d[0], (1100, 2.0))
        d = storage.get(key="test.ph", range_key=1200)
        self.assertEqual(d[0], (1200, 3.0))
        d = storage.get(key="test.ph", range_key=2000)
        self.assertEqual(d[0], (2000, 4.0))

        ds = storage.query(key="test.ph", range_min=1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-1000, range_max=1000)
        self.assertEqual(len(ds), 1)
        self.assertEqual(ds[0][0], (1000, 1.0))

        ds = storage.query(key="test.ph", range_min=-999, range_max=999)
        self.assertEqual(len(ds), 0)

        ds = storage.query(key="test.ph", range_min=1000, range_max=1200)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=1350)
        self.assertEqual(len(ds), 3)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=1101, range_max=1200)
        self.assertEqual(len(ds), 2)
        self.assertEqual(ds[0][0], (1100, 2.0))
        self.assertEqual(ds[1][0], (1200, 3.0))

        ds = storage.query(key="test.ph", range_min=99, range_max=999999)
        self.assertEqual(len(ds), 4)
        self.assertEqual(ds[0][0], (1000, 1.0))
        self.assertEqual(ds[1][0], (1100, 2.0))
        self.assertEqual(ds[2][0], (1200, 3.0))
        self.assertEqual(ds[3][0], (2000, 4.0))

        d = storage.last(key="test.ph")
        self.assertEqual(d[0], (2000, 4.0))

        d = storage.first(key="test.ph")
        self.assertEqual(d[0], (1000, 1.0))

        d = storage.left(key="test.ph", range_key=1050)
        self.assertEqual(d[0], (1000, 1.0))

        s = storage.range(key="test.ph")
        self.assertEqual(s["ts_min"], 1000)
        self.assertEqual(s["ts_max"], 2000)
