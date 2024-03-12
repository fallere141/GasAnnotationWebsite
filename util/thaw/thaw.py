# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

# Add utility code here
import boto3
from boto3.dynamodb.conditions import Attr, Key
import botocore
import requests
import re
import json
# from boto3 import exceptions
from botocore import exceptions
from botocore.errorfactory import ClientError

config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')


def query(user_id):
    # query by user_id to get all items of the user
    dynamo = boto3.resource(config['aws']['boto3Dynamo'])
    table = dynamo.Table(config['aws']['boto3DynamoTable'])
    response = table.query(
        IndexName="user_id_index",
        ExpressionAttributeValues={
            ':user_id': user_id,
        },
        KeyConditionExpression='user_id = :user_id',
    )
    return response["Items"]


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


def check(bucket_name, object_key):
    # check if the restore job finished

    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=bucket_name, Key=object_key)
    # https: // docs.aws.amazon.com / AmazonS3 / latest / API / API_HeadObject.html
    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / s3 / client / restore_object.html
    if 'Restore' in response:
        pattern = r'(\S+?)="(.*?)"'
        pairs = re.findall(pattern, response['Restore'])
        result = dict(pairs)
        if result["ongoing-request"] != "false":
            return False
        else:
            return True

    else:
        return False


def copy(bucket_name, object_key):
    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / s3 / client / copy_object.html
    s3 = boto3.client('s3')
    s3.copy_object(
        Bucket=bucket_name,
        Key=object_key,
        CopySource={'Bucket': bucket_name, 'Key': object_key},
        StorageClass='STANDARD'
    )


dynamo = boto3.resource(config['aws']['boto3Dynamo'])
table = dynamo.Table(config['aws']['boto3DynamoTable'])
queue = get_queues(config['aws']['glacierQueueThaw'])[0]

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
            # get response
            jsonResponse = json.loads(i.body)
            jsonResponse2 = json.loads(jsonResponse["Message"])

            # check if finished, then delete message and copy it to standard
            if check(jsonResponse2["s3_results_bucket"], jsonResponse2["s3_key_result_file"]):
                try:
                    copy(jsonResponse2["s3_results_bucket"], jsonResponse2["s3_key_result_file"])
                    i.delete()
                except ClientError:
                    continue

