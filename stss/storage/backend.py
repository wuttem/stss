#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals
import bisect
import os
import binascii
import logging
import json
from abc import ABCMeta, abstractmethod
from redis import StrictRedis as Redis
import boto3
import botocore
from collections import namedtuple
from ..errors import NotFoundError, ConflictError
from .models import Bucket


logger = logging.getLogger(__name__)


class StorageAPI(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def _to_bucket(self, item):
        pass

    @abstractmethod
    def _from_bucket(self, bucket):
        pass

    def get(self, key, range_key):
        return self._to_bucket(self._get(key, range_key))

    @abstractmethod
    def _get(self, key, range_key):
        pass

    def insert(self, bucket):
        self._insert(bucket.key, bucket.range_key, self._from_bucket(bucket))

    @abstractmethod
    def _insert(self, key, range_key, item):
        pass

    def update(self, bucket):
        self._update(bucket.key, bucket.range_key, self._from_bucket(bucket))

    @abstractmethod
    def _update(self, key, range_key, item):
        pass

    def query(self, key, range_min, range_max):
        out = list()
        for i in self._query(key, range_min, range_max):
            out.append(self._to_bucket(i))
        return out

    @abstractmethod
    def _query(self, key, range_min, range_max):
        pass

    def last(self, key, limit=1):
        assert limit < 10
        l = self._last(key, limit=limit)
        if limit == 1:
            return self._to_bucket(l[0])
        else:
            return [self._to_bucket(i) for i in l]

    @abstractmethod
    def _last(self, key, limit=1):
        pass

    def first(self, key, limit=1):
        assert limit < 10
        f = self._first(key, limit=limit)
        if limit == 1:
            return self._to_bucket(f[0])
        else:
            return [self._to_bucket(i) for i in f]

    @abstractmethod
    def _first(self, key, limit=1):
        pass

    def left(self, key, range_key, limit=1):
        assert limit < 10
        l = self._left(key, range_key, limit=limit)
        if limit == 1:
            return self._to_bucket(l[0])
        else:
            return [self._to_bucket(i) for i in l]

    @abstractmethod
    def _left(self, key, range_key, limit=1):
        pass

    def range(self, key):
        try:
            s = {"ts_min": self.ts_min(key),
                 "ts_max": self.ts_max(key)}
        except NotFoundError:
            return None
        else:
            return s

    def ts_min(self, key):
        i = self.first(key)
        return i.ts_min

    def ts_max(self, key):
        i = self.last(key)
        return i.ts_max

    def _count(self, key):
        items = self.query(key, 0, (2**31)-1)
        count = 0
        for i in items:
            count += i.count
        return count

    def count(self, key):
        return self._count(key)


class DynamoStorage(StorageAPI):
    def __init__(self, table_name,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 region_name=None, local_dynamo=False, create_table=False,
                 endpoint_url="http://localhost:8000"):
        kwargs = {}
        if aws_access_key_id:
            kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            kwargs["aws_secret_access_key"] = aws_secret_access_key
        if region_name:
            kwargs["region_name"] = region_name
        if local_dynamo:
            kwargs["aws_access_key_id"] = "none"
            kwargs["aws_secret_access_key"] = "none"
            kwargs["region_name"] = "none"
            kwargs["endpoint_url"] = endpoint_url

        self.local = local_dynamo
        self.table_name = "stss_{}".format(table_name)
        self.client = boto3.resource('dynamodb', **kwargs)
        self.client_low = boto3.client('dynamodb', **kwargs)
        self.table = self.client.Table(self.table_name)

    def _createTable(self):
        logger.warning("creating table %s", self.table_name)
        self.client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'key',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'range_key',
                    'AttributeType': 'N'
                }],
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'key',
                    'KeyType': 'HASH'
                }, {
                    'AttributeName': 'range_key',
                    'KeyType': 'RANGE'
                }],
            ProvisionedThroughput={
                'ReadCapacityUnits': 123,
                'WriteCapacityUnits': 123
            },
            StreamSpecification={
                'StreamEnabled': False,
#                'StreamViewType': 'NEW_IMAGE'|'OLD_IMAGE'|'NEW_AND_OLD_IMAGES'|'KEYS_ONLY'
            })

    def _dropTable(self):
        if not self.local:
            raise RuntimeError("I will not delete a not local table")
        if self.table:
            logger.warning("Deleting Table %s", self.table)
            try:
                self.table.delete()
            except botocore.exceptions.ClientError:
                logger.warning("could not delete table")

    def _to_bucket(self, item):
        key = item["key"]
        data = str(item["data"])
        bucket = Bucket.from_db_data(key, data)
        return bucket

    def _from_bucket(self, bucket):
        item =  {"key": bucket.key,
                 "range_key": bucket.range_key,
                 "data": bucket.to_string(),
                 "size": len(bucket)}
        return item

    def _insert(self, key, range_key, item):
        self.table.put_item(
            Item={
                'key': key,
                'range_key': range_key,
                'data': boto3.dynamodb.types.Binary(item["data"])
            },
            ConditionExpression=boto3.dynamodb.conditions.Attr("key").not_exists()
        )

    def _get(self, key, range_key):
        result = self.table.get_item(
            Key={
                'key': key,
                'range_key': range_key
            },
            ConsistentRead=True,
        )
        item = result.get("Item", None)
        if not item:
            raise NotFoundError
        return item

    def _first(self, key, limit=1):
        result = self.table.query(
            Select='ALL_ATTRIBUTES',
            Limit=limit,
            ConsistentRead=True,
            ScanIndexForward=True,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('key').eq(key))
        items = list(result["Items"])
        if len(items) < 1:
            raise NotFoundError
        return items

    def _last(self, key, limit=1):
        result = self.table.query(
            Select='ALL_ATTRIBUTES',
            Limit=limit,
            ConsistentRead=True,
            ScanIndexForward=False,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('key').eq(key))
        items = list(result["Items"])
        if len(items) < 1:
            raise NotFoundError
        return items

    def _left(self, key, range_key, limit=1):
        result = self.table.query(
            Select='ALL_ATTRIBUTES',
            Limit=limit,
            ConsistentRead=True,
            ScanIndexForward=False,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('key').eq(key) & boto3.dynamodb.conditions.Key('range_key').lte(range_key))
        items = list(result["Items"])
        if len(items) < 1:
            raise NotFoundError
        return items

    def _update(self, key, range_key, data, size):
        self._insert(key, range_key, data, size)

    def _full_query(self, ScanIndexForward=True, ConsistentRead=True,
                        KeyConditionExpression=None, Select=None):
        result = self.table.query(
            Select=Select,
            ConsistentRead=ConsistentRead,
            ScanIndexForward=ScanIndexForward,
            KeyConditionExpression=KeyConditionExpression)
        items = result['Items']

        while result.get('LastEvaluatedKey'):
            result = self.table.query(
                Select=Select,
                ConsistentRead=ConsistentRead,
                ScanIndexForward=ScanIndexForward,
                KeyConditionExpression=KeyConditionExpression,
                ExclusiveStartKey=result['LastEvaluatedKey'])
            items.extend(result['Items'])
        return list(items)

    def _query(self, key, range_min, range_max):
        items = self._full_query(
            Select='ALL_ATTRIBUTES',
            ConsistentRead=True,
            ScanIndexForward=True,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('key').eq(key) & boto3.dynamodb.conditions.Key('range_key').between(range_min, range_max))
        try:
            left = self._left(key, range_min, limit=1)[0]
        except NotFoundError:
            return items
        if len(items) > 0 and left == items[0]:
            pass
        else:
            items.insert(0, left)
        return items


class RedisStorage(StorageAPI):
    def __init__(self, redis=None, expire=None, **kwargs):
        if expire is not None:
            self.expire = expire
        else:
            self.expire = False
        if redis is not None:
            self.redis = redis
        else:
            self.redis = Redis(**kwargs)

    def _to_bucket(self, item):
        d = json.loads(item)
        return Bucket.from_db_data(d["key"], binascii.unhexlify(d["data"]))

    def _from_bucket(self, bucket):
        return json.dumps({"key": bucket.key,
                           "range_key": bucket.range_key,
                           "data": binascii.hexlify(bucket.to_string())})

    def _insert(self, key, range_key, item):
        self.redis.zadd(key, range_key, item)
        if self.expire:
            self.redis.expire(key, self.expire)

    def _get(self, key, range_key):
        l = self.redis.zrevrangebyscore(key, min=range_key, max=range_key,
                                        start=0, num=1)
        if len(l) < 1:
            raise NotFoundError
        return l[0]

    def _first(self, key, limit=1):
        i = self.redis.zrangebyscore(key, min="-inf", max="+inf",
                                     start=0, num=limit)
        if len(i) < 1:
            raise NotFoundError
        return i[:limit]

    def _last(self, key, limit=1):
        i = self.redis.zrevrangebyscore(key, min="-inf", max="+inf",
                                        start=0, num=limit)
        if len(i) < 1:
            raise NotFoundError
        return i[:limit]

    def _left(self, key, range_key, limit=1):
        i = self.redis.zrevrangebyscore(key, min="-inf", max=range_key,
                                        start=0, num=limit)
        if len(i) < 1:
            raise NotFoundError
        return i[:limit]

    def _update(self, key, range_key, item):
        p = self.redis.pipeline()
        p.zremrangebyscore(key, min=range_key, max=range_key)
        p.zadd(key, range_key, item)
        if self.expire:
            p.expire(key, self.expire)
        p.execute()

    def _query(self, key, range_min, range_max):
        items = self.redis.zrangebyscore(key, min=range_min, max=range_max)
        try:
            left = self._left(key, range_min)[0]
        except NotFoundError:
            return items
        if len(items) > 0 and left == items[0]:
            pass
        else:
            items.insert(0, left)
        return items


class FileStorage(StorageAPI):
    def __init__(self, path):
        self.storage_path = os.path.realpath(path)
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
        self.cache = {}

    def _to_bucket(self, item):
        return Bucket.from_db_data(item["key"], binascii.unhexlify(item["data"]))

    def _from_bucket(self, bucket):
        return {"key": bucket.key,
                "range_key": bucket.range_key,
                "data": binascii.hexlify(bucket.to_string())}

    def _load_key(self, key):
        o = []
        filename = os.path.join(self.storage_path, "{}.stss".format(key))
        if not os.path.isfile(filename):
            return o
        with open(filename, 'r') as f:
            for line in f:
                o.append(json.loads(line.strip()))
        self.cache[key] = o

    def _write_key(self, key):
        filename = os.path.join(self.storage_path, "{}.stss".format(key))
        with open(filename, 'w') as f:
            for i in self.cache[key]:
                f.write(json.dumps(i))
                f.write("\n")

    def _left(self, key, range_key, limit=1):
        self._load_key(key)
        idx = self._le(key, range_key) + 1
        idx_min = idx - limit
        return [x for x in self._slice(key, idx_min, idx)]

    def _get_key(self, key):
        if key not in self.cache:
            self.cache[key] = list()
        return self.cache[key]

    def _get_range_keys(self, key):
        return [r["range_key"] for r in self._get_key(key)]

    def _index(self, key, range_key):
        a = self._get_range_keys(key)
        i = bisect.bisect_left(a, range_key)
        if i != len(a) and a[i] == range_key:
            return i
        raise NotFoundError

    def _ge(self, key, range_min):
        a = self._get_range_keys(key)
        i = bisect.bisect_left(a, range_min)
        return i

    def _le(self, key, range_max):
        a = self._get_range_keys(key)
        i = bisect.bisect_right(a, range_max)
        if i and i > 0:
            return i - 1
        raise NotFoundError

    def _at(self, key, index):
        return self._get_key(key)[index]

    def _slice(self, key, min, max):
        return self._get_key(key)[min:max]

    def _insert(self, key, range_key, item):
        self._load_key(key)
        a = self._get_range_keys(key)
        position = bisect.bisect_left(a, range_key)
        if position != len(a) and a[position] == range_key:
            raise ConflictError
        self._get_key(key).insert(position, dict(key=key, range_key=range_key,
                                                 data=item["data"]))
        self._write_key(key)

    def _update(self, key, range_key, item):
        self._load_key(key)
        i = self._index(key, range_key)
        self._get_key(key)[i] = dict(key=key, range_key=range_key,
                                     data=item["data"])
        self._write_key(key)

    def _get(self, key, range_key):
        self._load_key(key)
        i = self._index(key, range_key)
        return self._at(key, i)

    def _query(self, key, range_min, range_max):
        self._load_key(key)
        m = self._ge(key, range_min)
        try:
            e = self._le(key, range_max) + 1
        except NotFoundError:
            return []
        # Get one before maybe there is a range key inside
        if m > 0:
            m -= 1
        return [x for x in self._slice(key, m, e)]

    def _last(self, key, limit=1):
        self._load_key(key)
        k = self._get_key(key)
        if len(k) > 0:
            return k[-limit:]
        raise NotFoundError

    def _first(self, key, limit=1):
        self._load_key(key)
        k = self._get_key(key)
        if len(k) > 0:
            return k[:limit]
        raise NotFoundError
