# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import datetime

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

import sys
import time
import boto3
import re
import os
from configparser import SafeConfigParser
from boto3.dynamodb.conditions import Attr, Key
import json
from botocore.exceptions import ClientError


def get_queues(prefix=None):
    # https: // docs.aws.amazon.com / code - library / latest / ug / sqs_example_sqs_ListQueues_section.html
    """
    Gets a list of SQS queues. When a prefix is specified, only queues with names
    that start with the prefix are returned.

    :param prefix: The prefix used to restrict the list of returned queues.
    :return: A list of Queue objects.
    """
    sqs = boto3.resource('sqs')

    if prefix:
        queue_iter = sqs.queues.filter(QueueNamePrefix=prefix)
    else:
        queue_iter = sqs.queues.all()
    queues = list(queue_iter)
    return queues


config = SafeConfigParser(os.environ)
config.read('archive_config.ini')
queue = get_queues(config['aws']['glacierQueue'])[0]

# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue
    messages = queue.receive_messages(
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    # If message read, extract job parameters from the message body as before
    if messages:
        for i in messages:
            jsonResponse = json.loads(i.body)
            dynamo = boto3.resource(config['aws']['boto3Dynamo'])
            table = dynamo.Table(config['aws']['boto3DynamoTable'])
            jsonResponse2 = json.loads(jsonResponse["Message"])
            try:
                job_id = jsonResponse2["job_id"]
            except ClientError:
                i.delete()
                continue
            current_time = datetime.datetime.now()
            # query from db and get the bucket name and object key
            response = table.query(KeyConditionExpression=Key("job_id").eq(job_id))
            bucket_name = response["Items"][0]["s3_results_bucket"]
            object_key = response["Items"][0]["s3_key_result_file"]
            if response["Items"][0]["user_role"] != "free_user":
                i.delete()
                continue
            # Initialize S3 client
            s3 = boto3.client('s3')

            # Copy the object to itself with Glacier storage class
            copy_source = {
                'Bucket': bucket_name,
                'Key': object_key
            }
            # Copy object with specified storage class
            try:
                # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / s3 / client / copy_object.html
                response = s3.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=object_key,
                    StorageClass='GLACIER',
                )
            except ClientError:
                continue
            i.delete()
