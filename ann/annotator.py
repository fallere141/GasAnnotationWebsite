from flask import Flask, request, render_template, jsonify
import uuid
import os
import subprocess
from botocore.config import Config
import logging
import boto3
from botocore.exceptions import ClientError
import re
from boto3.dynamodb.conditions import Attr, Key
import botocore
import json
from configparser import SafeConfigParser

from botocore import errorfactory


def update_running(uuid):
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    dynamo = boto3.resource(config['aws']['boto3Dynamo'])
    table = dynamo.Table(config['aws']['boto3DynamoTable'])
    try:
        # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / dynamodb / client / update_item.html
        response = table.update_item(
            Key={
                'job_id': uuid,
            },
            ConditionExpression=Attr('job_status').eq("PENDING"),
            UpdateExpression="set job_status = :r",
            ExpressionAttributeValues={
                ':r': 'RUNNING',
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError:
        return


def find_object_s3(uuid, user_id):
    # find the s3 object with prefix key
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    client = boto3.client(config['aws']['boto3S3'])

    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / s3 / client / list_objects.html
    response = client.list_objects(
        Bucket=config['aws']['inputBucket'],
        Prefix=config['aws']['prefix'] + user_id + "/" + uuid,
    )
    result = [i["Key"] for i in response["Contents"]]
    return result


def get_uid_user(obj_names):
    # get user_id and job_id from object key
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    searchObj = re.search(config['aws']['prefix'] + r'(.*)/(.*)~(.*)', obj_names, re.M | re.I)
    if searchObj:
        return [
            {"user": searchObj.group(1), "uid": searchObj.group(2), "name": searchObj.group(3), "objectURL": obj_names}]
    else:
        return None


def get_descriptions(uuid, user_id):
    # get the job info by job_id
    x = []
    for i in find_object_s3(uuid, user_id):
        x += get_uid_user(i)
    return x


def check_task_exist(uid: str):
    # check if the task is already exist
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    # client = boto3.client(config['aws']['boto3S3'])
    if os.path.exists(config['aws']['jobFolder'] + uid):
        return True
    else:
        return False


def get_not_init_task(obj_description):
    # get all job_id that hasn't been start

    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    s3 = boto3.client(config['aws']['boto3S3'])
    toBeInit = []
    for i in obj_description:
        if not check_task_exist(i["uid"]):
            os.mkdir(config['aws']['jobFolder'] + i["uid"])
            s3.download_file(config['aws']['inputBucket'], i["objectURL"],
                             config['aws']['jobFolder'] + i["uid"] + "/" + i["user"] + "~" + i["name"])
            toBeInit.append(i)
    return toBeInit


def command(uuid: str, user_id, user_email: str, user_role: str):

    # combine the command to start the annotation
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    obj_dict = get_descriptions(uuid, user_id)
    toBeinit = get_not_init_task(obj_dict)

    command_scritp = [
        config['aws']['pythonPath'] + " " + config['aws']['runPyPath'] + " " + config['aws']['jobFolder']
        + i["uid"] + "/" + i[
            "user"] + "~" + i["name"] + " " + user_email + " " + user_role for i in toBeinit]

    return {"commands": command_scritp, "object": toBeinit, "code": 200, "message": "success"}


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
config.read('ann_config.ini')
queue = get_queues(config['aws']['requestQueue'])[0]

# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue
    messages = queue.receive_messages(
        MessageAttributeNames=["All"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    # Use long polling - DO NOT use sleep() to wait between polls

    # If message read, extract job parameters from the message body as before
    if messages:
        for i in messages:
            jsonResponse = json.loads(i.body)
            dataJson = jsonResponse["Message"]
            data = json.loads(dataJson)
            # if job is already started by others
            if (data["job_status"] != "PENDING"):
                continue

            # start annotation
            commands = command(data["job_id"], data["user_id"], data["user_email"], data["user_role"])
            for c in commands["commands"]:
                ann_process = subprocess.Popen(c, shell=True)

            # update job status
            try:
                update_running(data["job_id"])
            except errorfactory.ConditionalCheckFailedException:
                continue
            # Delete the message from the queue, if job was successfully
            i.delete()
    # submitted
