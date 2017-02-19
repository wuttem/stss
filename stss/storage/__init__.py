#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals
import re
import logging
import redis

from .backend import FileStorage, RedisStorage, DynamoStorage
from .models import Bucket, ResultSet, BucketType
from ..errors import NotFoundError


logger = logging.getLogger(__name__)


class TSDB(object):
    def __init__(self, STORAGE="file", **kwargs):
        self.settings = {
            "BUCKET_TYPE": "daily",
            "BUCKET_DYNAMIC_TARGET": 100,
            "BUCKET_DYNAMIC_MAX": 200,
            "REDIS_PORT": 6379,
            "REDIS_HOST": "localhost",
            "REDIS_DB": 0,
            "ENABLE_CACHING": False,
            "ENABLE_EVENTS": False,
            "FILE_STORAGE_FOLDER": "./stss/",
            "DYNAMO_TABLE_NAME": "data_table",
            "DYNAMO_LOCAL": True
        }
        self.settings.update(kwargs)

        # Setup Item Model
        Bucket.DYNAMICSIZE_TARGET = self.settings["BUCKET_DYNAMIC_TARGET"]
        Bucket.DYNAMICSIZE_MAX = self.settings["BUCKET_DYNAMIC_MAX"]
        Bucket.DEFAULT_BUCKETTYPE = BucketType[self.settings["BUCKET_TYPE"]]

        # Setup Redis Pool
        self.redis_pool = redis.ConnectionPool(host=self.settings["REDIS_HOST"],
                                               port=self.settings["REDIS_PORT"],
                                               db=self.settings["REDIS_DB"])

        # Setup Storage
        if STORAGE == "file":
            self.storage = FileStorage()
        elif STORAGE == "redis":
            self.storage = RedisStorage(connection_pool=self.redis_pool)
        elif STORAGE == "dynamo":
            self.storage = DynamoStorage(table_name=self.settings["DYNAMO_TABLE_NAME"],
                                         local_dynamo=self.settings["DYNAMO_LOCAL"])
        else:
            raise NotImplementedError("Storage not implemented")

        # Event Class
        if self.settings["ENABLE_EVENTS"]:
            pass

        if self.settings["ENABLE_CACHING"]:
            pass


    def _get_last_item_or_new(self, key):
        # Get it from DB
        try:
            item = self.storage.last(key)
        except NotFoundError:
            item = Bucket.new(key)
        return item

    def _get_items_between(self, key, ts_min, ts_max):
        return self.storage.query(key, ts_min, ts_max)

    def query(self, key, ts_min, ts_max):
        return self._query(key, ts_min, ts_max)

    def _query(self, key, ts_min, ts_max):
        r = ResultSet(key, self._get_items_between(key, ts_min, ts_max))
        r._trim(ts_min, ts_max)
        return r

    def _insert_or_update_item(self, item):
        if item.existing:
            self.storage.update(item)
        else:
            self.storage.insert(item)

    def insert_bulk(self, inserts):
        res = []
        for i in inserts:
            res.append(self._insert(i["key"], i["data"]))
        return res

    def insert(self, key, data):
        return self._insert(key, data)

    def _insert(self, key, data):
        key = key.lower()
        if not re.match(r'^[A-Za-z0-9_\-\.]+$', key):
            raise ValueError("Key should be alphanumeric (including .-_)")

        assert(isinstance(data, list))
        assert(len(data) > 0)
        data.sort(key=lambda x: x[0])

        # Limits and Stats
        ts_min = int(data[0][0])
        ts_max = int(data[-1][0])
        count = len(data)
        logger.debug("Inserting {} {} points".format(key, len(data)))
        logger.debug("Limits: {} - {}".format(ts_min, ts_max))
        stats = {"ts_min": ts_min, "ts_max": ts_max, "count": count,
                 "appended": 0, "inserted": 0, "updated": 0, "key": key,
                 "splits": 0, "merged": 0}

        # Find the last Item
        last_item = self._get_last_item_or_new(key)
        if len(last_item) > 0:
            last_item_range_key = last_item.range_key
        else:
            last_item_range_key = -1
        logger.debug("Last: {}".format(last_item))

        # List with all Items we updated
        updated = []

        # Just Append - Best Case
        if ts_min >= last_item.ts_max:
            logger.debug("Append Data")
            appended = last_item.insert(data)
            updated.append(last_item)
            stats["appended"] += appended
        else:
            # Merge Round
            merge_items = self._get_items_between(key, ts_min, ts_max)
            assert(len(merge_items) > 0)
            assert(merge_items[0].ts_min <= ts_min)
            logger.debug("Merging Data Query({} - {}) {} items"
                         .format(ts_min, ts_max, len(merge_items)))
            i = len(data) - 1
            m = len(merge_items) - 1
            inserted = 0
            while i >= 0:
                last_merge_item = merge_items[m]
                if data[i][0] >= last_merge_item.ts_min:
                    inserted += last_merge_item.insert_point(data[i][0],
                                                             data[i][1])
                    i -= 1
                else:
                    m -= 1
            updated += merge_items
            stats["merged"] += len(merge_items)
            stats["inserted"] += inserted

        # Splitting Round
        updated_splitted = []
        for i in updated:
            # Check Size for Split
            if not i.split_needed(limit="soft"):
                logger.debug("No Split, No Fragmentation")
                updated_splitted.append(i)
            # If its not the last we let it grow a bit
            elif i != last_item and not i.split_needed(limit="hard"):
                logger.debug("Fragmentation, No Split")
                updated_splitted.append(i)
            else:
                splited = i.split_item()
                logger.debug("Split needed")
                for j in splited:
                    updated_splitted.append(j)
                stats["splits"] += 1

        # Update
        if stats["inserted"] > 0 or stats["appended"] > 0:
            # Update Round
            for i in updated_splitted:
                self._insert_or_update_item(i)
        else:
            logger.info("Duplicate ... Nothing to do ...")

        return stats