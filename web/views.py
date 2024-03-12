# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
                   request, session, url_for)
from boto3.dynamodb.conditions import Attr, Key
from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile
import re
import botocore
import requests

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


def create_presigned_url(bucket_name, object_name, expiration=3600):
    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / guide / s3 - presigned - urls.html
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except botocore.exceptions.ClientError as e:
        # logging.error(e)
        return None

    # The response contains the presigned URL
    return response


def get_topic(name: str):
    # get a topic it's prefix is name
    sns = boto3.resource("sns")
    topic_list = sns.topics.all()
    for i in topic_list:
        if re.search(name, i.arn):
            return i
    return None


def find_object_s3(key):
    # find the s3 object with prefix key
    client = boto3.client("s3")

    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / s3 / client / list_objects.html
    response = client.list_objects(
        Bucket=app.config['AWS_S3_INPUTS_BUCKET'],
        Prefix=key
    )
    result = [[i["Key"], i["LastModified"]] for i in response["Contents"]]
    return result


def get_uid_user(obj_names):
    # get user_id and job_id from object key
    searchObj = re.search(app.config['AWS_S3_KEY_PREFIX']+r'(.*)/(.*)~(.*)', obj_names, re.M | re.I)
    if searchObj:
        return [
            {"user": searchObj.group(1), "uid": searchObj.group(2), "name": searchObj.group(3), "objectURL": obj_names}]
    else:
        return None


@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client('s3',
                      region_name=app.config['AWS_REGION_NAME'],
                      config=Config(signature_version='s3v4'))

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']

    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
               str(uuid.uuid4()) + '~${filename}'

    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    result = find_object_s3(key)[0]
    description = get_uid_user(result[0])[0]



    # get user profile
    profile = get_profile(identity_id=session.get('primary_identity'))
    data = {"job_id": description["uid"],
            "user_id": description["user"],
            "s3_inputs_bucket": bucket_name,
            "input_file_name": description["name"],
            "s3_key_input_file": description["objectURL"],
            "submit_time": int(result[1].utcnow().timestamp()),
            "job_status": "PENDING",
            "user_email": profile.email,
            "user_role": profile.role,
            }


    # Persist job to database
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.put_item(Item=data)

    # Send message to request queue
    topic = get_topic(app.config['AWS_SNS_JOB_REQUEST_TOPIC'])
    data["user_email"] = profile.email
    data["user_role"] = profile.role
    dataJson = json.dumps(data)
    topic.publish(Message=dataJson)

    return render_template('annotate_confirm.html', job_id=description["uid"])


"""List all annotations for the user
"""


@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # Get list of annotations to display

    # query by user_id in dynamodb
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / dynamodb / client / query.html
    response = table.query(
        IndexName="user_id_index",
        ExpressionAttributeValues={
            ':user_id': session['primary_identity'],
        },
        KeyConditionExpression='user_id = :user_id',
    )


    return render_template('annotations.html', annotations=response["Items"])


"""Display details of a specific annotation job
"""


@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    dynamo = boto3.resource('dynamodb')
    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / dynamodb / client / query.html
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

    # query by job_id to get job detail
    response = table.query(KeyConditionExpression=Key("job_id").eq(id))

    # if user not who is login know return No authorized
    if response["Items"][0]["user_id"] != session['primary_identity']:
        return render_template('error.html', message="Not authorized to view this job", alert_level="danger")
    else:
        responseData = response["Items"][0]
        data = {"job_id": responseData["job_id"],
                "submit_time": responseData["submit_time"],
                "input_file_url": create_presigned_url(responseData["s3_inputs_bucket"],
                                                       responseData["s3_key_input_file"], 300),
                "input_file_name": responseData["input_file_name"],
                "result_file_url": create_presigned_url(responseData["s3_results_bucket"],
                                                        responseData["s3_key_result_file"], 300),
                "job_status": responseData["job_status"],
                "complete_time": responseData["complete_time"],
                "submit_time": responseData["submit_time"],
                }
        return render_template('annotation_details.html', annotation=data)


"""Display the log file contents for an annotation job
"""


@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    dynamo = boto3.resource('dynamodb')
    # https: // boto3.amazonaws.com / v1 / documentation / api / latest / reference / services / dynamodb / client / query.html
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    # query by job_id to get job detail
    response = table.query(KeyConditionExpression=Key("job_id").eq(id))

    # if user not who is login know return No authorized
    if response["Items"][0]["user_id"] != session['primary_identity']:
        return render_template('error.html', message="Not authorized to view this job", alert_level="danger")
    else:
        responseData = response["Items"][0]
        url = create_presigned_url(responseData["s3_results_bucket"], responseData["s3_key_log_file"])
        if url is not None:

            # get response with text to show on pages
            response = requests.get(url)
            return render_template('view_log.html', job_id=id, log_file_contents=response.text)
        else:
            return render_template('error.html', message="We meet some error here", alert_level="danger")


"""Subscription management handler
"""


@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if (request.method == 'GET'):
        # Display form to get subscriber credit card info
        if (session.get('role') == "free_user"):
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif (request.method == 'POST'):
        # Update user role to allow access to paid features
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Make sure you handle files not yet archived!

        # when update to premium, send a message to queue to restore glacier
        restore_topic = get_topic(app.config['AWS_SNS_GLACIER_TOPIC'])

        restore = {"user_id": session['primary_identity']}
        restore_json = json.dumps(restore)
        response = restore_topic.publish(Message=restore_json)

        # Display confirmation page
        return render_template('subscribe_confirm.html')


"""Reset subscription
"""


@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""


@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')


"""Login page; send user to Globus Auth
"""


@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html',
                           title='Page not found', alert_level='warning',
                           message="The page you tried to reach does not exist. \
      Please check the URL and try again."
                           ), 404


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
                           title='Not authorized', alert_level='danger',
                           message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
                           ), 403


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
                           title='Not allowed', alert_level='warning',
                           message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
                           ), 405


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
                           title='Server error', alert_level='danger',
                           message="The server encountered an error and could \
      not process your request."
                           ), 500

### EOF
