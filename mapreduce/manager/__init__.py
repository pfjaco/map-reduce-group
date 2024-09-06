"""Python package emulating a manager node for distributed mapreduce."""

from mapreduce.utils import network, PathJSONEncoder
from mapreduce.manager.__main__ import Manager
from mapreduce.worker.__main__ import Worker
