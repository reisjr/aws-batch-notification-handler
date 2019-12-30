import boto3
import json
import os
import gzip
import logging
import uuid

BATCH_QUEUE_URL = str(os.environ.get("BATCH_QUEUE_URL", "DEFAULT_VALUE"))
BATCH_SIZE = 10

s3_cli = boto3.client("s3")
sqs_cli = boto3.client("sqs")

logging.getLogger('boto').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer.tasks').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer.utils').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer.futures').setLevel(logging.CRITICAL)

logger = logging.getLogger()
logger.setLevel("INFO")

def download_files_from_s3(s3_cli, event):
    files = []
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        download_path = '/tmp/{}'.format(uuid.uuid4())
        logger.info("Downloading object {}/{} to {}".format(bucket, key, download_path))
        s3_cli.download_file(bucket, key, download_path)
        files.append(download_path)
    
    return files


def safe_read_line(fp):
    line = ""
    try:
        line = fp.readline()
    except Exception as e:
        logger.error("Error reading line", exc_info=True)
        print(e)
        
    return line

    
def lambda_handler(event, context):
    logger.debug(json.dumps(event))

    #download S3 file
    files = download_files_from_s3(s3_cli, event)

    for file in files:
        with open(file) as fp:
            cnt = 0
            line = safe_read_line(fp)
            batches_sent = 1
            
            while line:
                entries_in_batch = 0
                entries = []

                while line and entries_in_batch < BATCH_SIZE:
                
                    entry = {
                        'Id': str(cnt), 
                        'MessageBody': str(line)
                    }
                    
                    entries.append(entry)
                    line = safe_read_line(fp)
                    cnt += 1
                    entries_in_batch += 1
        
                if (cnt + 1) % 500 == 0:
                    logger.info("batches_sent = {} / cnt = {}.".format(batches_sent, cnt))
                
                r = sqs_cli.send_message_batch(
                    QueueUrl=BATCH_QUEUE_URL,
                    Entries=entries
                )
                
                batches_sent += 1
                

    return "OK"


if __name__ == "__main__":
    lambda_handler(None, None)
