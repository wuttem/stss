#!/usr/bin/python
# coding: utf8
from __future__ import print_function

try:
    from itertools import izip as zip
except ImportError:  # will be 3.x series
    pass

from enum import Enum
from collections import MutableSequence
from collections import namedtuple
from collections import OrderedDict
from itertools import chain

import bisect
import logging
import struct
import array
import hashlib
import json

from .helper import ts_daily_left, ts_daily_right
from .helper import ts_hourly_left, ts_hourly_right
from .helper import ts_weekly_left, ts_weekly_right
from .helper import ts_monthly_left, ts_monthly_right


Aggregation = namedtuple('Aggregation', ['min', 'max', 'sum', 'count'])


class BucketType(Enum):
    dynamic = 1
    hourly = 2
    daily = 3
    weekly = 4
    monthly = 5
    resultset = 6


class ItemType(Enum):
    raw_float = 1
    raw_int = 2
    tuple_float_2 = 3
    tuple_float_3 = 4
    tuple_float_4 = 5
    basic_aggregation = 6


class TupleArray(MutableSequence):
    def __init__(self, data_type="f", tuple_size=2):
        if tuple_size < 2 or tuple_size > 20:
            raise ValueError("invalid tuple size (2-20)")
        super(TupleArray, self).__init__()
        self.data_type = data_type
        self.tuple_size = tuple_size
        self._arrays = [array.array(data_type) for i in range(tuple_size)]

    def __len__(self):
        return len(self._arrays[0])

    def __getitem__(self, ii):
        return tuple(item[ii] for item in self._arrays)

    def __delitem__(self, ii):
        for a in self._arrays:
            del a[ii]

    def __setitem__(self, ii, val):
        if len(val) != len(self._arrays):
            raise ValueError("tuple size incorrect")

        for i, v in enumerate(val):
            self._arrays[i][ii] = v
        return tuple(item[ii] for item in self._arrays)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<TupleArray {} x {}>".format(self.data_type, self.tuple_size)

    def insert(self, ii, val):
        if len(val) != len(self._arrays):
            raise ValueError("tuple size incorrect")

        for i, v in enumerate(val):
            self._arrays[i].insert(ii, v)

    def append(self, val):
        if len(val) != len(self._arrays):
            raise ValueError("tuple size incorrect")

        for i, v in enumerate(val):
            self._arrays[i].append(v)

    def tostring(self):
        return b"".join([x.tostring() for x in self._arrays])

    def fromstring(self, string):
        s = len(string) / len(self._arrays)
        for i, a in enumerate(self._arrays):
            f = int(i * s)
            t = int(i * s + s)
            a.fromstring(string[f:t])


class Bucket(object):
    def __init__(self, parent, key, range_key, values=None):
        self.parent = parent
        self._dirty = False
        self._existing = False
        self._range_min = 0
        self._range_max = 0
        self.set_range_key(range_key)

        # Create Data Structures
        self._timestamps = array.array("I")
        if self.item_type == ItemType.raw_float:
            self._values = array.array("f")
        elif self.item_type == ItemType.raw_int:
            self._values = array.array("I")
        elif self.item_type == ItemType.tuple_float_2:
            self._values = TupleArray("f", 2)
        elif self.item_type == ItemType.tuple_float_3:
            self._values = TupleArray("f", 3)
        elif self.item_type == ItemType.tuple_float_4:
            self._values = TupleArray("f", 4)
        else:
            raise NotImplementedError("invalid item type")

        if values is not None:
            self.insert(values)

    @property
    def item_type(self):
        return self.parent.item_type

    @property
    def bucket_type(self):
        return self.parent.bucket_type

    @property
    def key(self):
        return self.parent.key

    @property
    def existing(self):
        return self._existing

    @property
    def dirty(self):
        return self._dirty

    def reset_dirty(self):
        self._dirty = False

    @property
    def range_key(self):
        return self._range_min

    def set_range_key(self, range_key):
        if self.bucket_type == BucketType.hourly:
            l = ts_hourly_left(range_key)
            r = ts_hourly_right(range_key)
        elif self.bucket_type == BucketType.daily:
            l = ts_daily_left(range_key)
            r = ts_daily_right(range_key)
        elif self.bucket_type == BucketType.weekly:
            l = ts_weekly_left(range_key)
            r = ts_weekly_right(range_key)
        elif self.bucket_type == BucketType.monthly:
            l = ts_monthly_left(range_key)
            r = ts_monthly_right(range_key)
        else:
            raise NotImplementedError("invalid bucket type")
        if l != range_key:
            raise ValueError("invalid range key: %s" % range_key)
        self._range_min = l
        self._range_max = r

    @property
    def range_min(self):
        return self._range_min

    @property
    def range_max(self):
        return self._range_max

    def __len__(self):
        return len(self._timestamps)

    def __bool__(self):  # Python 3
        if len(self) < 1:
            return False
        if len(self._timestamps) != len(self._values):
            return False
        # Check if sorted
        it = iter(self._timestamps)
        it.__next__()
        return all(b >= a for a, b in zip(self._timestamps, it))

    def __nonzero__(self):  # Python 2
        if len(self) < 1:
            return False
        if len(self._timestamps) != len(self._values):
            return False
        # Check if sorted
        it = iter(self._timestamps)
        it.next()
        return all(b >= a for a, b in zip(self._timestamps, it))

    def to_hash(self):
        s = "{}.{}.{}.{}.{}.{}.{}.{}".format(self.key, self.item_type,
                                             self.bucket_type, len(self), 
                                             self.ts_min, self.ts_max, 
                                             self.existing, self.dirty)
        return hashlib.sha1(s).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, Bucket):
            return False
        # Is Hashing a Performance Problem ?
        # h1 = self.to_hash()
        # h2 = other.to_hash()
        # return h1 == h2
        # This would compare the objects without hash
        if self.key != other.key:
            return False
        if self._dirty != other._dirty:
            return False
        if self.item_type != other.item_type:
            return False
        if self.bucket_type != other.bucket_type:
            return False
        if len(self._timestamps) != len(other._timestamps):
            return False
        if len(self._timestamps) > 0:
            if self._timestamps[0] != other._timestamps[0]:
                return False
            if self._timestamps[-1] != other._timestamps[-1]:
                return False
        return True

    def __ne__(self, other):
        return not self == other  # NOT return not self.__eq__(other)

    def __repr__(self):
        l = len(self._timestamps)
        if l > 0:
            m = self._timestamps[0]
        else:
            m = -1
        return "<{} series({}), min_ts: {}, items: {}, buckets: {}>".format(
            self.key, l, m, self.item_type, self.bucket_type)

    @property
    def ts_max(self):
        if len(self._timestamps) > 0:
            return self._timestamps[-1]
        return -1

    @property
    def ts_min(self):
        if len(self._timestamps) > 0:
            return self._timestamps[0]
        return -1

    def _at(self, i):
        return (self._timestamps[i], self._values[i])

    def __getitem__(self, key):
        return self._at(key)

    def to_string(self):
        header = (struct.pack("H", int(self.item_type.value)) +
                  struct.pack("H", int(self.bucket_type.value)))
        length = struct.pack("I", len(self))
        return (header + length + self._timestamps.tostring() +
                self._values.tostring())

    @classmethod
    def from_string(cls, key, string):
        item_type = ItemType(int(struct.unpack("H", string[0:2])[0]))
        bucket_type = BucketType(int(struct.unpack("H", string[2:4])[0]))
        item_length = int(struct.unpack("I", string[4:8])[0])
        split = 8 + 4 * item_length
        ts, v = string[8:split], string[split:]
        i = Bucket(key, item_type=item_type, bucket_type=bucket_type)
        i._timestamps.fromstring(ts)
        i._values.fromstring(v)
        assert(i)
        return i

    def insert_point(self, timestamp, value, overwrite=False):
        timestamp = int(timestamp)
        idx = bisect.bisect_left(self._timestamps, timestamp)
        # Append
        if idx == len(self._timestamps):
            self._timestamps.append(timestamp)
            self._values.append(value)
            self._dirty = True
            return 1
        # Already Existing
        if self._timestamps[idx] == timestamp:
            # Replace
            logging.debug("duplicate insert")
            if overwrite:
                self._values[idx] = value
                self._dirty = True
                return 1
            return 0
        # Insert
        self._timestamps.insert(idx, timestamp)
        self._values.insert(idx, value)
        self._dirty = True
        return 1

    def insert(self, series):
        counter = 0
        for timestamp, value in series:
            counter += self.insert_point(timestamp, value)
        return counter


class BucketCollection(OrderedDict):
    def __init__(self, parent, *args, **kwargs):
        self.parent = parent
        super(BucketCollection, self).__init__(*args, **kwargs)

    def __missing__(self, key):
        k = self.parent.key
        bucket = Bucket(self.parent, k, key)
        self[key] = bucket
        return self[key]


class TimeSeries(object):
    DEFAULT_ITEMTYPE = ItemType.raw_float
    DEFAULT_BUCKETTYPE = BucketType.daily

    def __init__(self, key, values=None):
        # Determine Types
        # Maybe get this from key
        self.item_type = self.DEFAULT_ITEMTYPE
        self.bucket_type = self.DEFAULT_BUCKETTYPE

        self.key = str(key).lower()

        self.buckets = BucketCollection(self)
        if values is not None:
            self.insert(values)

    def get_range_left(self, timestamp):
        if self.bucket_type == BucketType.hourly:
            return ts_hourly_left(timestamp)
        elif self.bucket_type == BucketType.daily:
            return ts_daily_left(timestamp)
        elif self.bucket_type == BucketType.weekly:
            return ts_weekly_left(timestamp)
        elif self.bucket_type == BucketType.monthly:
            return ts_monthly_left(timestamp)
        else:
            raise NotImplementedError("invalid bucket type")

    def get_range_right(self, timestamp):
        if self.bucket_type == BucketType.hourly:
            return ts_hourly_right(timestamp)
        elif self.bucket_type == BucketType.daily:
            return ts_daily_right(timestamp)
        elif self.bucket_type == BucketType.weekly:
            return ts_weekly_right(timestamp)
        elif self.bucket_type == BucketType.monthly:
            return ts_monthly_right(timestamp)
        else:
            raise NotImplementedError("invalid bucket type")

    def insert(self, series):
        last_range_min = -1
        last_range_max = -1
        for timestamp, value in series:
            if last_range_min <= timestamp <= last_range_max:
                # just insert
                self.buckets[last_range_min].insert_point(timestamp, value)
            else:
                l = self.get_range_left(timestamp)
                r = self.get_range_right(timestamp)
                if l < last_range_min or r < last_range_max:
                    raise ValueError("unsorted range key")
                last_range_min = l
                last_range_max = r
                self.buckets[last_range_min].insert_point(timestamp, value)

    @property
    def timestamps(self):
        bucket_timestamps = [x._timestamps for x in self.buckets.itervalues()]
        return chain(bucket_timestamps)

    @property
    def values(self):
        bucket_values = [x._values for x in self.buckets.itervalues()]
        return chain(bucket_values)

    def __len__(self):
        return sum([len(x) for x in self.buckets.itervalues()])

    def _at(self, i):
        offset = 0
        idx = 0
        buckets = list(self.buckets.items())
        current_bucket = buckets[idx]
        while i >= len(current_bucket) + offset:
            offset += len(current_bucket)
            idx += 1
            current_bucket = buckets[idx]
        return current_bucket[i-offset]

    def __getitem__(self, key):
        return self._at(key)


class ResultSet(TimeSeries):
    def __init__(self, key, items):
        super(ResultSet, self).__init__(key)
        self.bucket_type = BucketType.resultset
        for i in items:
            if i.key != key:
                raise ValueError("Item has wrong key")
            self._timestamps += i._timestamps
            self._values += i._values

    def _trim(self, ts_min, ts_max):
        low = bisect.bisect_left(self._timestamps, ts_min)
        high = bisect.bisect_right(self._timestamps, ts_max)
        self._timestamps = self._timestamps[low:high]
        self._values = self._values[low:high]

    def all(self):
        """Return an iterater to get all ts value pairs.
        """
        return zip(self._timestamps, self._values)

    def daily(self):
        """Generator to access daily data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._timestamps):
            j = 0
            lower_bound = ts_daily_left(self._timestamps[i])
            upper_bound = ts_daily_right(self._timestamps[i])
            while (i + j < len(self._timestamps) and
                   lower_bound <= self._timestamps[i + j] <= upper_bound):
                j += 1
            yield ((self._timestamps[x], self._values[x])
                   for x in range(i, i + j))
            i += j

    def hourly(self):
        """Generator to access hourly data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._timestamps):
            j = 0
            lower_bound = ts_hourly_left(self._timestamps[i])
            upper_bound = ts_hourly_right(self._timestamps[i])
            while (i + j < len(self._timestamps) and
                   lower_bound <= self._timestamps[i + j] <= upper_bound):
                j += 1
            yield ((self._timestamps[x], self._values[x])
                   for x in range(i, i + j))
            i += j

    def aggregation(self, group="hourly", function="mean"):
        """Aggregation Generator.
        """
        if group == "hourly":
            it = self.hourly
            left = ts_hourly_left
        elif group == "daily":
            it = self.daily
            left = ts_daily_left
        else:
            raise ValueError("Invalid aggregation group")

        if function == "sum":
            func = sum
        elif function == "count":
            func = len
        elif function == "min":
            func = min
        elif function == "max":
            func = max
        elif function == "amp":
            def amp(x):
                return max(x) - min(x)
            func = amp
        elif function == "mean":
            def mean(x):
                return sum(x) / len(x)
            func = mean
        else:
            raise ValueError("Invalid aggregation group")

        for g in it():
            t = list(g)
            ts = left(t[0][0])
            value = func([x[1] for x in t])
            yield (ts, value)