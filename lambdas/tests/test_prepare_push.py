from prepare_push.lambda_function import *
from tests import *
import json
import boto3
import datetime


def test_get_query_time_2h():
    event_time = datetime.datetime.fromisoformat("2019-12-12T10:10:10.518993+00:00")
    t_delta = datetime.timedelta(hours=2)
    query_prefix = get_query_time(event_time, t_delta)
    
    assert "2019-12-12T08:1" == query_prefix


def test_get_query_time_24h():
    t_delta = datetime.timedelta(hours=24)
    event_time = datetime.datetime.fromisoformat("2019-12-12T10:10:10.518993+00:00")
    query_prefix = get_query_time(event_time, t_delta)
    
    assert "2019-12-11T10:1" == query_prefix


