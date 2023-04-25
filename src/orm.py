from datetime import datetime, timedelta
import os
import traceback
from boto3.dynamodb.conditions import Key as DDBKey

from sneks.ddb import make_json_safe
import sneks.snekjson as json
from sneks.ddb.orm import CFObject

_DataObject = CFObject.lazysubclass(stack_name=os.environ["STACK_NAME"], logical_name="DataTable")

def clean_key(k):
    k = k.lower()
    k = k.replace(":","").replace("-","")
    k = k.strip().replace(" ","_")
    return k

SOURCE_EVENTS= "EVENTS"

IGNORE = [
    "NOAA Space Weather Scale descriptions can be found at",
    "www.swpc.noaa.gov/noaa-scales-explanation"
]
EASY_TO_PARSE = [
        "ALERT: ",
        "Active Warning: ",
        "Aurora - ",
        "Begin Time: ",
        "CANCEL WARNING: ",
        "CONTINUED ALERT: ",
        "Cancel Serial Number: ",
        "Comment: ",
        "Continuation of Serial Number: ",
        "Description: ",
        "Deviation: ",
        "EXTENDED WARNING: ",
        "End Time: ",
        "Estimated Velocity: ",
        "Extension to Serial Number: ",
        "IP Shock Passage Observed: ",
        "Induced Currents - ",
        "Issue Time: ",
        "Location: ",
        "Maximum 10MeV Flux: ",
        "Maximum Time: ",
        "NOAA Scale: ",
        "Navigation - ",
        "Now Valid Until: ",
        "Observed: ",
        "Optical Class: ",
        "Original Issue Time: ",
        "Potential Impacts: ",
        "Radio - ",
        "SUMMARY: ",
        "Serial Number: ",
        "Space Weather Message Code: ",
        "Spacecraft - ",
        "Station: ",
        "Synoptic Period: ",
        "Threshold Reached: ",
        "Valid From: ",
        "Valid To: ",
        "WARNING: ",
        "WATCH: ",
        "Warning Condition: ",
        "X-ray Class: ",
        "Yesterday Maximum 2MeV Flux: ",
    ]

def parse_event(msg):
    lines = msg.split("\r\n")
    lines = [l.strip() for l in lines if l.strip()]
    data = {}
    last_line_handled = -1
    unhandled_lines = []
    for i in range(len(lines)):
        handled = False
        if i <= last_line_handled:
            # this is just used in a couple cases where the contents of one line tell us how to parse the following line
            continue
        line = lines[i]
        if line in IGNORE:
            continue
        if line == "THIS SUPERSEDES ANY/ALL PRIOR WATCHES IN EFFECT":
            data["supersedes"] = line
            continue
        if line == "Highest Storm Level Predicted by Day:":
            next_line = lines[i+1].replace(":  ",": ").replace(":  ",": ")
            days = [d.split(": ") for d in next_line.split("   ")]
            days = {x[0].strip():x[1].strip() for x in days}
            data[clean_key(line)] = days
            last_line_handled = i+1
            continue
        if line.startswith("Potential Impacts: ") and not line.startswith("Potential Impacts: Satellite") and not line.startswith("Potential Impacts: Area"):
            sline = line[len("Potential Impacts: "):].strip()
            for prefix in ["Radio - ","Induced Currents - ","Aurora - ","Spacecraft - ","Navigation - "]:
                if sline.startswith(prefix):
                    data[clean_key(prefix)] = sline[len(prefix):].strip()
                    handled = True
        poleward_impact = "Potential Impacts: Area of impact primarily poleward of "
        if line.startswith(poleward_impact):
            # record this separately but still also parse this line with the later parser
            data["latitude"] = line[len(poleward_impact):].strip().split(" ")[0]
        for prefix in EASY_TO_PARSE:
            if line.startswith(prefix):
                data[clean_key(prefix)] = line[len(prefix):].strip()
                handled = True
        if not handled:
            print(f"UNHANDLED LINE: {line}")
            unhandled_lines.append(line)
    if unhandled_lines:
        data["unhandled_lines"] = unhandled_lines
    return data

def parse_event_timestamp(tss):
    # "issue_datetime": "2023-04-24 17:55:48.160"
    return datetime.strptime(tss, "%Y-%m-%d %H:%M:%S.%f000").timestamp()

class DataObject(_DataObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def parse_event(cls, contents, save=False):
        product_id = contents.get("product_id", "X00X")
        timestamp = parse_event_timestamp(contents["issue_datetime"])
        source = SOURCE_EVENTS
        message = contents.get("message","?")
        datum = cls.__init__(source=source, timestamp=timestamp, product_id=product_id, message=message, data=parse_event(message))
        if save:
            datum.save()
        return datum

    @property
    def pretty_timestamp(self):
        dt = datetime.fromtimestamp(self["timestamp"])
        dt_string = dt.strftime("%Y/%m/%d %H:%M:%S")
        return dt_string

    @classmethod
    def latest_event(cls):
        return cls.latest_entry(source=SOURCE_EVENTS)

    @classmethod
    def latest_entry(cls, source):
        response = cls.query(source=source, ScanIndexForward=False, MaxResults=1)
        if response.get("Items"):
            return response["Items"][0]
        return None

    @classmethod
    def load_range(cls, source, start=None, end=None, count=0, oldest=False):
        kwargs = {"ScanIndexForward":oldest}
        if count > 0:
            kwargs["Limit"] = count
        hash_key_condition = DDBKey("source").eq(source)
        if start or end:
            start = start if start else 0
            end = end if end else time.time()
            range_key_condition = DDBKey("timestamp").between(decimal.Decimal(start), decimal.Decimal(end))
            kwargs["KeyConditionExpression"] = hash_key_condition & range_key_condition
        else:
            kwargs["KeyConditionExpression"] = hash_key_condition
        return cls.query_all(**kwargs)

    @classmethod
    def load_n_events(cls, source, n, oldest=False):
        return cls.load_range(source=source, count=n, oldest=oldest)

    @classmethod
    def all_since(cls, source, timestamp):
        return cls.load_range(source, start=timestamp, oldest=False)
