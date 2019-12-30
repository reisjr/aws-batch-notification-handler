from __future__ import print_function

import boto3
import botocore
from botocore.client import Config
from botocore.exceptions import ClientError
import json
import datetime
import traceback
import sys
import os 
import logging.config
import time
import dateutil.parser
#from urlparse import urlparse
from urllib.parse import urlparse
from lambda_helper.lambda_function_helper import json_dumps_decimal, upload_file
import gzip

# ENV VARIABLES
LOG_LEVEL = str(os.environ.get("LOG_LEVEL", "DEBUG")).upper()
QUERY_OUTPUT_LOCATION = str(os.environ.get("QUERY_OUTPUT_LOCATION", "DEFAULT_VALUE"))
DATABASE = str(os.environ.get("DATABASE", "telconew"))
TABLE_NAME= "batch_notification_dreisdboardingestbucket544bc58_1wzzinreurze7"

# CONST
LOG_LEVEL_DEFAULT = "DEBUG"
BATCH_SIZE = 5000

logging.config.fileConfig('logging.ini')
logging.getLogger('boto').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer.tasks').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer.utils').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer.futures').setLevel(logging.CRITICAL)

# Load logger
logger = logging.getLogger()

# Load AWS clients
athena_cli = boto3.client("athena")
sqs_cli = boto3.client("sqs")
s3_res = boto3.resource("s3")


def setup_log(level):
    try:
        logging.root.setLevel(level)
    except ValueError:
        logging.root.error('Invalid log level: %s', level)
        level = LOG_LEVEL_DEFAULT
        logging.root.setLevel(level)

    boto_level = LOG_LEVEL_DEFAULT

    try:
        logging.getLogger('boto').setLevel(boto_level)
        logging.getLogger('boto3').setLevel(boto_level)
        logging.getLogger('botocore').setLevel(boto_level)
    except ValueError:
        logging.root.error('Invalid log level: %s', boto_level)


def get_query_time(event_time, t_delta):
    
    query_time = event_time - t_delta
    query_prefix = query_time.isoformat()[0:15]
    
    logger.debug("            QUERY TIME: {}".format(query_time))
    logger.debug("          QUERY PREFIX: {}".format(query_prefix))

    return query_prefix


def save_gzip(query_execution_id, batch_count, entries):
    with gzip.open("/tmp/{}_batch_{}.gz".format(query_execution_id, batch_count), 'wb') as f:
        logger.debug("Writing batch {} / {}".format(batch_count, query_execution_id))
        for entry in entries:
            f.write("{}\n".format(entry).encode())
    return


def save_uncompressed(query_execution_id, batch_count, entries):
    with open("/tmp/{}_batch_{}".format(query_execution_id, batch_count), 'w') as f:
        logger.debug("Writing batch {} / {}".format(batch_count, query_execution_id))
        for entry in entries:
            f.write("{}\n".format(entry))
    return


def lambda_handler(event, context):

    logger.debug("Input:\n{}".format(json.dumps(event)))
    logger.debug("             DATABASE: {}".format(DATABASE))
    logger.debug("QUERY_OUTPUT_LOCATION: {}".format(QUERY_OUTPUT_LOCATION))
    logger.debug("           EVENT TIME: {}".format(event["time"]))

    event_time_str = event["time"]
    event_time = dateutil.parser.parse(event_time_str)

    t_delta = datetime.timedelta(hours=2)
    #t_delta = datetime.timedelta(hours=24)

    query_prefix = get_query_time(event_time, t_delta)
    
    r = athena_cli.start_query_execution(
        QueryString="SELECT msisdn, event" + \
            "FROM \"{}\".\"{}\"" + \
            "WHERE SUBSTRING(ts, 1, 15) = '{}' AND " + \
            "1 == 1 -- MSISDN NOT IN (SELECT ...) FILTER PURCHASE EVENTS".format(DATABASE, TABLE_NAME, query_prefix),
        QueryExecutionContext={
            "Database": DATABASE
        },
        ResultConfiguration={
            "OutputLocation": QUERY_OUTPUT_LOCATION
        }
    )

    query_execution_id = r["QueryExecutionId"]
    
    time.sleep(1)
    
    r = athena_cli.get_query_execution(
        QueryExecutionId=query_execution_id
    )
    
    finished = False
    
    state = r["QueryExecution"]["Status"]["State"]
    
    cnt = 0
    
    while state != "SUCCEEDED" and cnt < 15:
        time.sleep(1)

        r = athena_cli.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        logger.debug(json_dumps_decimal(r))
        
        state = r["QueryExecution"]["Status"]["State"]
        cnt += 1
    
    if state != "SUCCEEDED":
        return "ERROR processing prefix = '{}".format(query_prefix)

    s3_obj = r["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    exec_time = r["QueryExecution"]["Statistics"]["EngineExecutionTimeInMillis"]
    data_scanned = r["QueryExecution"]["Statistics"]["DataScannedInBytes"]
    
    logger.debug("   S3 OBJ: {}".format(s3_obj))
    logger.debug("EXEC TIME: {}".format(exec_time))
    logger.debug("DATA SCAN: {}".format(data_scanned))
    
    o = urlparse(s3_obj, allow_fragments=False)

    logger.info("BUCKET: {}".format(o.netloc))
    logger.info("  PATH: {}".format(o.path.lstrip('/')))

    for i in range(0, 5):
        try:
            s3_res.meta.client.download_file(o.netloc, o.path.lstrip('/'), "/tmp/{}.csv".format(query_execution_id))
            break
        except Exception:
            logger.warn("File not ready")            

        time.sleep(1)

    batch_count = 0

    with open("/tmp/{}.csv".format(query_execution_id)) as fp:
        line = fp.readline() # remove header
        cnt = 0

        while line:
            entries = []
            batch_items = 0
            
            while line and batch_items < BATCH_SIZE:
                msisdn = line.split(",")[0]
                cnt += 1
                batch_items += 1
                entries.append(msisdn)
                line = fp.readline()

            save_uncompressed(query_execution_id, batch_count, entries)
            #save_gzip(query_execution_id, batch_count, entries)
            
            batch_count += 1

            if cnt % 500 == 0:
                logger.debug("{}.".format(cnt))

        logger.info("TOTAL: {}".format(cnt))

    # UPLOAD
    for i in range(0, batch_count):
        if upload_file("/tmp/{}_batch_{}".format(query_execution_id, i), o.netloc, object_name="batches/{}/batch_{}.csv".format(query_execution_id, i)):
            logger.info("UPLOADED {} / {}".format(i, query_execution_id))
        else:
            logger.error("FAILED {} / {}".format(i, query_execution_id))
            
    return "OK - batch_count {} - records {}".format(batch_count, cnt)


if __name__ == "__main__":
    lambda_handler(None, None)