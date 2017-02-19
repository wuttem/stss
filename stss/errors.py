#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals


class NotFoundError(KeyError):
    pass


class ConflictError(ValueError):
    pass


class InternalError(RuntimeError):
    pass
