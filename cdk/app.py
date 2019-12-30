#!/usr/bin/env python3

from aws_cdk import core

from batch_notification.batch_notification import BatchNotificationStack

app = core.App()
BatchNotificationStack(app, "batch-notification", env={'region': 'us-east-2'})

app.synth()
