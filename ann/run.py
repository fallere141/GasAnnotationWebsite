# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import boto3
import re
import os
from configparser import SafeConfigParser
from boto3.dynamodb.conditions import Attr, Key
import json

"""A rudimentary timer for coarse-grained profiling
"""

def get_topic(name: str):
    sns = boto3.resource("sns")

    topic_list = sns.topics.all()
    for i in topic_list:
        if re.search(name, i.arn):
            return i
    return None

class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


# test.annot.vcf  test.vcf  test.vcf.count.log

if __name__ == '__main__':
    config = SafeConfigParser(os.environ)
    config.read('ann_config.ini')
    if len(sys.argv) > 3:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

            # get file path
            filepath = sys.argv[1]
            searchObj = re.search(r'(.*)/task/(.*)/(.*)~(.*)\.(.*)', filepath, re.M | re.I)

            path = searchObj.group((1)) + "/task/"
            uid = searchObj.group((2))
            user = searchObj.group((3))
            name = searchObj.group((4))
            s3 = boto3.client(config['aws']['boto3S3'])
            filename1 = path + uid + "/" + user + "~" + name + ".vcf.count.log"
            obj_path1 = config['aws']['prefix'] + user + "/" + uid + "~" + name + ".vcf.count.log"

            # upload files
            with open(filename1, "rb") as f:
                s3.upload_fileobj(f, config['aws']['resultBucket'], obj_path1)

            filename2 = path + uid + "/" + user + "~" + name + ".annot.vcf"
            obj_path2 = config['aws']['prefix'] + user + "/" + uid + "~" + name + ".annot.vcf"
            with open(filename2, "rb") as f:
                s3.upload_fileobj(f, config['aws']['resultBucket'], obj_path2)

            dynamo = boto3.resource(config['aws']['boto3Dynamo'])
            table = dynamo.Table(config['aws']['boto3DynamoTable'])
            job_id = uid

            # update job detail
            response = table.update_item(
                Key={
                    'job_id': job_id,
                },
                ConditionExpression=Attr('job_status').eq("RUNNING"),
                UpdateExpression="set job_status = :r, s3_results_bucket = :k, s3_key_result_file = :l, s3_key_log_file= :p, complete_time = :o",
                ExpressionAttributeValues={
                    ':r': 'COMPLETED',
                    ':k':  config['aws']['resultBucket'],
                    ':l': obj_path2,
                    ':p': obj_path1,
                    ':o': int(time.time()),

                },
                ReturnValues="UPDATED_NEW"
            )

            # send email to user
            topic = get_topic(config['aws']['resultQueue'])
            # sys argv 2 is user email
            dataJson = json.dumps({"user_email": sys.argv[2], "job_id": job_id})
            topic.publish(Message=dataJson)

            if (sys.argv[3] == "free_user"):
                # if job is not free user(subscribe when annotating), then not put it into glacier message queue
                topic2 = get_topic(config['aws']['glacierTopic'])
                dataJson2 = json.dumps({"job_id": job_id})
                topic2.publish(Message=dataJson2)

            # remove file and directory from task directory
            os.remove(filename1)
            os.remove(filename2)
            os.remove(filepath)
            os.removedirs(path + uid + "/")
