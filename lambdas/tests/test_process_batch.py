from process_batch.lambda_function import *
from tests import *
import json
import boto3

s3_cli = boto3.client("s3")


def load_event(file):
    data = ""
    
    with open(file) as json_file:
        data = json.load(json_file)
    
    return data


def test_download():
    event = load_event("tests/s3_put_event.json")
    resp = download_files_from_s3(s3_cli, event)
    
    assert len(resp) == 1


def test_batch_queue():
    event = load_event("tests/s3_put_event.json")
    lambda_handler(event, None)
    
    #assert len(resp) == 1