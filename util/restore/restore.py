# restore.py
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

### EOF

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
config.read('restore_config.ini')


def restore(bucket_name, object_key):
    # Initialize S3 and SQS clients
    s3 = boto3.client('s3')

    restore_tier = config['aws']['restore_tier_Expedited']
    restore_request = {
        'Days': 1,
        'GlacierJobParameters': {
            'Tier': restore_tier
        }
    }

    # Initiate the restore request
    try:
        # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / s3 / client / restore_object.html
        response = s3.restore_object(
            Bucket=bucket_name,
            Key=object_key,
            RestoreRequest=restore_request
        )
    except ClientError:
        # if Expedited throws than try to use Standard
        restore_tier = config['aws']['restore_tier_Standard']
        response = s3.restore_object(
            Bucket=bucket_name,
            Key=object_key,
            RestoreRequest=restore_request
        )


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


def update_prem(uuid):
    # update item to be premium_user
    # so that annotator will not put it into glacier when finished
    dynamo = boto3.resource(config['aws']['boto3Dynamo'])
    table = dynamo.Table(config['aws']['boto3DynamoTable'])

    response = table.update_item(
        Key={
            'job_id': uuid,
        },
        UpdateExpression="set user_role = :r",
        ExpressionAttributeValues={
            ':r': 'premium_user',
        },
        ReturnValues="UPDATED_NEW"
    )


def get_topic(name: str):
    # get topic by name prefix
    sns = boto3.resource("sns")
    topic_list = sns.topics.all()
    for i in topic_list:
        if re.search(name, i.arn):
            return i
    return None


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



dynamo = boto3.resource(config['aws']['boto3Dynamo'])
table = dynamo.Table(config['aws']['boto3DynamoTable'])
queue = get_queues(config['aws']['glacierQueueRestore'])[0]
thaw_topic = get_topic(config['aws']['glacierQueueThaw'])
# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue
    messages = queue.receive_messages(
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    if messages:
        # If message read, extract job parameters from the message body as before
        for i in messages:
            jsonResponse = json.loads(i.body)
            jsonResponse2 = json.loads(jsonResponse["Message"])
            response = table.query(
                IndexName="user_id_index",
                ExpressionAttributeValues={
                    ':user_id': jsonResponse2["user_id"],
                },
                KeyConditionExpression='user_id = :user_id',
            )
            # for every job result start restore.
            for j in response["Items"]:
                # if job is not complete, update it, then run.py will not ask to put it into glacier
                if j["job_status"] != "COMPLETED":
                    update_prem(j["job_id"])
                else:
                    # start restore
                    update_prem(j["job_id"])
                    try:
                        restore(j["s3_results_bucket"], j["s3_key_result_file"])
                    except ClientError:
                        continue
                    thaw_info = {"s3_results_bucket": j["s3_results_bucket"],
                                 "s3_key_result_file": j["s3_key_result_file"]}
                    thaw_json = json.dumps(thaw_info)
                    # publish a topic to thaw, notify that this job is restoring
                    response = thaw_topic.publish(Message=thaw_json)
            i.delete()
