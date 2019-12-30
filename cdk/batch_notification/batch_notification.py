from aws_cdk import (
    core, aws_dynamodb, aws_lambda,
    aws_iam, aws_s3, aws_kinesis,
    aws_sqs,
    aws_kinesisfirehose,
    aws_kinesisanalytics,
    aws_lambda_event_sources,
    aws_events
)
from aws_cdk.aws_ec2 import SubnetType, Vpc
from aws_cdk.core import App, Construct, Duration
import json


class BatchNotificationStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        #kds_input_stream = aws_kinesis.Stream(self, "kds_dashboard_input_stream",
        #    shard_count=1, 
        #    stream_name="kds_dashboard_input_stream")
        
        kds_output_stream = aws_kinesis.Stream(self, "kds_dashboard_output_stream",
            shard_count=1, 
            stream_name="kds_dashboard_output_stream")

        # Creating a ingest bucket for this stack
        ingest_bucket = aws_s3.Bucket(self,"dreis_dboard_ingest_bucket")

        queue = aws_sqs.Queue(self, "DeliveryQueue", 
            queue_name="NotifQueue",
        )

        kfh_service_role = aws_iam.Role(self, "KFH_Dashboard_Role",
            assumed_by=aws_iam.ServicePrincipal("firehose.amazonaws.com")
        )

        kfh_policy_stmt = aws_iam.PolicyStatement(
            actions=["*"],
            resources=["*"]
        )

        kfh_service_role.add_to_policy(kfh_policy_stmt)

        #Creating firehose for this stack
        #kfh_source = aws_kinesisfirehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
        #    kinesis_stream_arn=kds_input_stream.stream_arn,
        #    role_arn=kfh_service_role.role_arn
        #)

        kfh_datalake = aws_kinesisfirehose.CfnDeliveryStream(self, "kfh_datalake",
            s3_destination_configuration=aws_kinesisfirehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=ingest_bucket.bucket_arn,
                buffering_hints=aws_kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=5),
                #compression_format="UNCOMPRESSED",
                compression_format="GZIP",
                role_arn=kfh_service_role.role_arn
                ),
            delivery_stream_type="DirectPut",
            #kinesis_stream_source_configuration=kfh_source
        )

        #aws_events.EventPattern.

        lambda_query_events = aws_lambda.Function(self, "QueryEvents",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            handler="lambda_function.lambda_handler",
            code=aws_lambda.Code.asset("../lambdas/prepare_push/"),
            timeout=Duration.minutes(5))

        lambda_query_events.add_environment("QUERY_OUTPUT_LOCATION", queue.queue_url)
        lambda_query_events.add_environment("DATABASE", queue.queue_url)

        lambda_prep_batches = aws_lambda.Function(self, "PrepareBatches",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            handler="lambda_function.lambda_handler",
            code=aws_lambda.Code.asset("../lambdas/process_batch/"),
            timeout=Duration.minutes(5))

        lambda_prep_batches.add_environment("BATCH_QUEUE_URL", queue.queue_url)

        #lambda_agg_function.add_environment("DDB_TABLE_DASHBOARD", table.table_name)

        lambda_query_events.add_to_role_policy(aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "glue:GetTable",
                "glue:GetPartitions",
                "s3:*"
            ],
            resources=["*"]
        ))

        lambda_prep_batches.add_to_role_policy(aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=[
                "sqs:*",
                "s3:GetObject"
            ],
            resources=["*"]
        ))

        #s3_evt_source = aws_lambda_event_sources.S3EventSource(
        #    bucket=ingest_bucket.bucket_name,
        #    events=[aws_s3.EventType.OBJECT_CREATED]
        #)

        #lambda_prep_batches.add_event_source(s3_evt_source)

        core.CfnOutput(
            self, "BucketName_Dashboard",
            description="Bucket name",
            value=ingest_bucket.bucket_arn
        )

        core.CfnOutput(
            self, "KinesisFHInputStream",
            description="Kinesis input for Dashboard",
            value=kfh_datalake.attr_arn
        )

        core.CfnOutput(
            self, "KinesisOutputStream_Dashboard",
            description="Kinesis output for Dashboard",
            value=kds_output_stream.stream_name
        )

        core.CfnOutput(
            self, "QueueName",
            description="Queue Name",
            value=queue.queue_name
        )
        
        core.CfnOutput(
            self, "QueueURL",
            description="Queue URL",
            value=queue.queue_url
        )

        