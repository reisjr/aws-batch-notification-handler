# Batch Notification Handler
A repo with a sample code for processing batch events such as MISSED CALL and PLAN PURCHASE.

# Build
To build this app, you need to be in this example's root folder. Then run the following:

```
$ cd cdk
$ python3 -m venv .env
$ source .env/bin/activate
$ pip install -r requirements.txt
```

This will install the necessary CDK, then this example's dependencies, and then build your Python files and your CloudFormation template.

Install the latest version of the AWS CDK CLI:

```
$ npm i -g aws-cdk
```

# Directories

## batch_notification

Generates random MISSED CALLS and PURCHASE events. Requires a Kinesis Firehose Stream.

## lambdas

### parse_events

Triggered using Cloudwatch events. Queries events on the last X hours using Athena and prepare batches with specific sizes for further processing.

### queue_events

Triggered using S3 uploads, enqueues messages on SQS.