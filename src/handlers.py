import base64
import boto3
import copy
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import http.cookies
import json
from math import radians, degrees, cos, sin, asin, sqrt, fabs, log, tan, pi, atan2
import os
import random
import requests
import traceback
import urllib
import urllib.parse
import uuid
import time

from sneks.sam import events
from sneks.sam.response_core import make_response, redirect, ApiException
from sneks.sam.decorators import register_path, returns_json, returns_html
from sneks.sam.exceptions import *
from sneks.ddb import deepload

from orm import *

from utils import log_function

DT_FORMATS = [
    "%Y/%m/%dT%H:%M:%S",
    "%Y/%m/%dT%H:%M:%S.%f",
]

def string_to_datetime(s):
    for frmt in DT_FORMATS:
        try:
            return datetime.strptime(s, frmt)
        except:
            continue
    raise RuntimeError("Unable to parse datetime string '{}'!".format(s))

def sanitize(s):
    return s.replace('"',"")

SNS = boto3.client("sns")
TOPIC_ARN = os.environ["TOPIC_ARN"]

def publish(message):
    response = SNS.publish(
        TopicArn=TOPIC_ARN,
        # PhoneNumber='string',
        Message=message,
        # Subject='string',
        # MessageStructure='string',
    )

def notify(obj, should_publish=False):
    message = obj.notification()
    if not message:
        return
    l = len(message)
    print(f"Generated message (l={l}):")
    print(message)
    if should_publish:
        print("Publishing...")
        publish(message)
        print("Published.")

def scrape_stuff(event, *args, **kwargs):
    events = requests.get("https://services.swpc.noaa.gov/products/alerts.json").json()
    print(json.dumps(events, sort_keys=True))
    for event in events:
        try:
            obj = EventObject.parse_event(event)
            print(f"Event found: {obj['space_weather_message_code']}/{obj['serial_number']} ({obj.pretty_timestamp})")
            obj.save()
            notify(obj, should_publish=obj.get("data",{}).get("latitude",90) < 56)
        except:
            message = traceback.format_exc()
            if "ConditionalCheckFailedException" in message:
                print("Reached the events that have already been saved.")
                break
            else:
                print("Unexpected error!")
                print(message)

# @register_path("HTML", r"^/?events/list/?$")
# @returns_html("events/list.html")
# def events_list_page(event, *args, next_token=None, **kwargs):
#     response = EntityObject.scan(NextToken=next_token)
#     entities = response["Items"]
#     token = response["NextToken"]
#     return {"entities":entities, "next_token":token}

# @register_path("HTML", r"^/?entity/view/?$")
# @returns_html("entity/view.html")
# @log_function
# def entity_view_page(event, *args, entity=None, next_token=None, **kwargs):
#     entity_obj = EntityObject.from_id(entity)
#     response = PostObject.query(IndexName="entity-timestamp-index", entity=entity, NextToken=next_token, ScanIndexForward=False)
#     posts = response["Items"]
#     token = response["NextToken"]
#     return {"entity":entity_obj, "posts":posts, "next_token":next_token}

def get_cookies(event):
    cookie_dict = {}
    try:
        cookies = http.cookies.SimpleCookie()
        cookies.load(event["headers"].get("Cookie",""))
        for k in cookies:
            morsel = cookies[k]
            cookie_dict[morsel.key] = morsel.value
    except:
        traceback.print_exc()
    return cookie_dict

def _add_info_kwargs(info, kwargs):
    if not kwargs:
        return info
    existing_kwargs = list(info.keys())
    for k in kwargs:
        if k not in existing_kwargs:
            info[k] = kwargs[k]
    return info

def add_body_as_kwargs(info, *args, **kwargs):
    if not info["event"].get("body"):
        info["body"] = {}
        return info
    body = json.loads(info["event"]["body"])
    info["body"] = body
    return _add_info_kwargs(info, body)

def add_qs_as_kwargs(info, *args, **kwargs):
    qs_args = info["event"]["queryStringParameters"]
    info["qs_args"] = qs_args
    return _add_info_kwargs(info, qs_args)
