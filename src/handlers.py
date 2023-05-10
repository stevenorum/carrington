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
from sneks.sam.decorators import register_path, returns_json, returns_html, returns_text
from sneks.sam.exceptions import *
from sneks.ddb import deepload

from grabber import Grabber
from orm import *

from utils import log_function

SHORT_FORECAST_URL = "https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt"
MONTH_FORECAST_URL = "https://services.swpc.noaa.gov/text/27-day-outlook.txt"
ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"

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
# TOPIC_ARN = os.environ["TOPIC_ARN"]
PAGE_TOPIC_ARN = os.environ["PAGE_TOPIC_ARN"]
TEXT_TOPIC_ARN = os.environ["TEXT_TOPIC_ARN"]
EMAIL_TOPIC_ARN = os.environ["EMAIL_TOPIC_ARN"]

# def publish(message):
#     response = SNS.publish(
#         TopicArn=TOPIC_ARN,
#         # PhoneNumber='string',
#         Message=message,
#         # Subject='string',
#         # MessageStructure='string',
#     )

def send_text(message):
    response = SNS.publish(
        TopicArn=TEXT_TOPIC_ARN,
        # PhoneNumber='string',
        Message=message,
        # Subject='string',
        # MessageStructure='string',
    )

def send_page(message):
    response = SNS.publish(
        TopicArn=PAGE_TOPIC_ARN,
        # PhoneNumber='string',
        Message=message,
        # Subject='string',
        # MessageStructure='string',
    )

def send_email(message, subject):
    response = SNS.publish(
        TopicArn=EMAIL_TOPIC_ARN,
        # PhoneNumber='string',
        Message=message,
        Subject=subject,
        # MessageStructure='string',
    )

def notify(obj, should_publish=False):
    message = obj.text_notification()
    if message:
        l = len(message)
        print(f"Generated SMS message (l={l}):")
        print(message)
        if should_publish:
            print("Publishing SMS...")
            # publish(message)
            send_text(message)
            print("Published SMS.")
    else:
        print("No SMS generated for event.")
    subject, body = obj.email_notification()
    if subject and body:
        print(f"Generated email message:\nSUBJECT: {subject}\nBODY:\n{body}")
        if should_publish:
            print("Publishing email...")
            send_email(message=body, subject=subject)
            print("Published email.")
    else:
        print("No email generated for event.")

def scrape_events():
    events = requests.get(ALERTS_URL).json()
    # print(json.dumps(events, sort_keys=True))
    for event in events:
        try:
            obj = EventObject.parse_event(event)
            print(f"Event found: {obj['space_weather_message_code']}/{obj['serial_number']} ({obj.pretty_timestamp})")
            obj.save()
            notify(obj, should_publish=obj.get("data",{}).get("latitude",90) < 51)
        except:
            message = traceback.format_exc()
            if "ConditionalCheckFailedException" in message:
                print("Reached the events that have already been saved.")
                break
            else:
                print("Unexpected error!")
                print(message)

def scrape_short_forecast():
    forecast = requests.get(SHORT_FORECAST_URL).text.replace("\r","")
    ForecastObject.from_short_forecast(forecast, save=True)

def scrape_month_forecast():
    forecast = requests.get(MONTH_FORECAST_URL).text.replace("\r","")
    ForecastObject.from_month_forecast(forecast, save=True)

def scrape_stuff(event, *args, **kwargs):
    try:
        print("Scraping events...")
        scrape_events()
    except:
        traceback.print_exc()
    try:
        print("Scraping short-term forecast...")
        scrape_short_forecast()
    except:
        traceback.print_exc()
    try:
        print("Scraping long-term forecast...")
        scrape_month_forecast()
    except:
        traceback.print_exc()

@register_path("HTML", r"^/?scrape/?$")
@returns_text
def scrape_now(event, *args, **kwargs):
    grabber = Grabber()
    with grabber:
        scrape_stuff(event, *args, **kwargs)
    info = grabber.info()
    ind = ">>>>>>>>>>"
    output = f"{ind}STDOUT:\n{info['stdout']}\n{ind}STDERR:\n{info['stderr']}\n{ind}LOGS:\n{info['logs']}\n{ind}DURATION (s):\n{info['duration']}"
    return output

@register_path("HTML", r"^/?events/list/?$")
@returns_html("events/list.html")
def events_list_page(event, *args, next_token=None, **kwargs):
    response = EventObject.query_chronological(NextToken=next_token)
    events = response["Items"]
    token = response["NextToken"]
    return {"events":events, "next_token":token}

@register_path("HTML", r"^/?events/(?P<space_weather_message_code>[A-Z0-9]{5,12})/?$")
@returns_html("events/list_code.html")
def events_list_code_page(event, space_weather_message_code, *args, next_token=None, **kwargs):
    response = EventObject.query(space_weather_message_code=space_weather_message_code, NextToken=next_token, ScanIndexForward=False)
    events = response["Items"]
    token = response["NextToken"]
    return {"events":events, "next_token":token, "space_weather_message_code":space_weather_message_code}

@register_path("HTML", r"^/?events/(?P<space_weather_message_code>[A-Z0-9]{5,12})/(?P<serial_number>[0-9]+)/?$")
@returns_html("events/view.html")
def event_view_page(event, space_weather_message_code, serial_number, test_notification=None, *args, **kwargs):
    serial_number = int(serial_number)
    event = EventObject.load(space_weather_message_code=space_weather_message_code, serial_number=serial_number)
    if test_notification:
        notify(event, should_publish=True)
    # can't have "event" as a key in the params handed back because it conflicts with the APIGateway event.
    return {"_event":event, "space_weather_message_code":space_weather_message_code, "serial_number":serial_number}

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
