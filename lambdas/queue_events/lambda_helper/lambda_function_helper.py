import decimal
from random import choice
from string import ascii_uppercase
import json
import datetime
import boto3

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):

        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def json_dumps_decimal(j):
    return json.dumps(j, cls=DecimalEncoder)


def create_random_name(size=8):
    return ''.join(choice(ascii_uppercase) for i in range(size))


def check_error_response(response, msg="No additional info"):
    http_code = 0
    
    if "ResponseMetadata" in response and "HTTPStatusCode" in response["ResponseMetadata"]:
        http_code = response["ResponseMetadata"]["HTTPStatusCode"]
    
    if http_code != 200:
        logger.debug(response)
        raise Exception('Invalid response code {} / {}'.format(http_code, msg))


def generate_response(params):
    return {
        'statusCode': 200,
        'body': json.dumps(params)
    }


def generate_error_response(op):
    return {
        'statusCode': 500,
        'body': json.dumps({
            "error-message": op
        })
    }

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True