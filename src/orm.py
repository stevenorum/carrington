from datetime import datetime, timedelta
import os
import traceback
from boto3.dynamodb.conditions import Key as DDBKey

from sneks.ddb import make_json_safe
import sneks.snekjson as json
from sneks.ddb.orm import CFObject, ensure_ddbsafe

_EventObject = CFObject.lazysubclass(stack_name=os.environ["STACK_NAME"], logical_name="EventTable")
_ForecastObject = CFObject.lazysubclass(stack_name=os.environ["STACK_NAME"], logical_name="ForecastTable")

def clean_key(k):
    k = k.lower()
    k = k.replace(":","").replace("-","")
    k = k.strip().replace(" ","_")
    return k

def clean_line(l):
    cl = l.replace("\r","").replace("\n","").replace("  "," ").strip()
    if len(cl) < len(l):
        return clean_line(cl)
    return l

def clean_lines(t):
    lines = [clean_line(l) for l in t.split("\n")]
    return [l for l in lines if l]

def time_left_in_year(dt):
    new_year = datetime(year=dt.year+1, month=1, day=1)
    td = new_year-dt
    return td.total_seconds()

def get_dt(issued_dt, day_str):
    year = issued_dt.year
    if day_str.startswith("Jan") and issued_dt.month == 12:
        year = year + 1
    return datetime.strptime(f"{year} {day_str}", "%Y %b %d") # 2023 Apr 30

def date_to_s(dt):
    return dt.strftime("%Y/%m/%d")

def format_date(s):
    # Input: 2023 May 01
    # Output: 2023/05/01
    return datetime.strptime(s,"%Y %b %d").strftime("%Y/%m/%d")

RF_KEY = "rf_forecasts"
AP_KEY= "ap_forecasts"
KP_KEY = "kp_forecasts"
STORM_KEY = "storm_forecasts"

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
            data["latitude"] = int(line[len(poleward_impact):].strip().split(" ")[0])
        for prefix in EASY_TO_PARSE:
            if line.startswith(prefix):
                data[clean_key(prefix)] = line[len(prefix):].strip()
                handled = True
        if not handled:
            print(f"UNHANDLED LINE: {line}")
            unhandled_lines.append(line)
    for x in ["valid_from", "valid_to", "issue_time", "now_valid_until","threshold_reached"]:
        if x in data:
            ts = data[x]
            dt = datetime.strptime(ts, "%Y %b %d %H%M %Z")
            data[x] = dt.strftime("%Y/%m/%dT%H:%MZ")
    if unhandled_lines:
        data["unhandled_lines"] = unhandled_lines
    return data

def parse_event_timestamp(tss):
    # "issue_datetime": "2023-04-24 17:55:48.160"
    return datetime.strptime(tss, "%Y-%m-%d %H:%M:%S.%f").timestamp()

class EventObject(_EventObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['serial_number'] = int(self['serial_number'])

    @classmethod
    def parse_event(cls, contents, save=False):
        product_id = contents.get("product_id", "X00X")
        timestamp = parse_event_timestamp(contents["issue_datetime"])
        source = SOURCE_EVENTS
        message = contents.get("message","?")
        data = parse_event(message)
        space_weather_message_code = data["space_weather_message_code"]
        serial_number = int(data["serial_number"])
        event = cls(
            space_weather_message_code=space_weather_message_code,
            serial_number=serial_number,
            source=source,
            timestamp=timestamp,
            product_id=product_id,
            message=message,
            data=data
        )
        if save:
            event.save()
        return event

    @property
    def pretty_timestamp(self):
        dt = datetime.fromtimestamp(self["timestamp"])
        dt_string = dt.strftime("%Y/%m/%d %H:%M:%S")
        return dt_string

    @property
    def summary(self):
        data = self["data"]
        for k in ["alert","watch","warning","summary","extended_warning","cancel_warning","continued_alert"]:
            if k in data:
                pk = k.upper().replace("_"," ")
                return f"{pk}: {data[k]}"
        return "-"

    # @classmethod
    # def latest_event(cls):
    #     return cls.latest_entry(source=SOURCE_EVENTS)

    @classmethod
    def latest_n_events(cls, n):
        response = cls.query(IndexName="source-timestamp-index", source=SOURCE_EVENTS, ScanIndexForward=False, MaxResults=n)
        return response.get("Items",[])

    @classmethod
    def query_chronological(cls, **kwargs):
        return cls.query(IndexName="source-timestamp-index", source=SOURCE_EVENTS, ScanIndexForward=False, **kwargs)

    def url(self):
        return f"https://apps.didelphisresearch.org/carrington/events/{self['data']['space_weather_message_code']}/{self['data']['serial_number']}"

    def text_notification(self, url=False):
        data = self["data"]
        if "aurora" not in data or "latitude" not in data:
            return None
        middle = self.url() if url else data['aurora']
        message = f"{data['space_weather_message_code']}/{data['serial_number']}\n{middle}\nGMLat:{data['latitude']}\n"
        if "valid_from" in data:
            message += f"from:{data['valid_from']} "
        if "valid_to" in data:
            message += f"to:{data['valid_to']}"
        if len(message) > 160:
            message = message.replace("latitude","lat")
        if len(message) > 160:
            message = message.replace("GMLat","GML")
        if len(message) > 160:
            message = message.replace("northern","N")
        return message

    def email_notification(self):
        data = self["data"]
        if "aurora" not in data or "latitude" not in data:
            return None
        subject = self.summary
        # url = f"https://apps.didelphisresearch.org/carrington/events/{data['space_weather_message_code']}/{data['serial_number']}"
        message = f"{self['message']}\n\n{self.url()}"
        return subject, message

    # @classmethod
    # def latest_entry(cls, source):
    #     response = cls.query(IndexName="source-timestamp-index", source=source, ScanIndexForward=False, MaxResults=1)
    #     if response.get("Items"):
    #         return response["Items"][0]
    #     return None

    # @classmethod
    # def load_range(cls, source, start=None, end=None, count=0, oldest=False):
    #     kwargs = {"ScanIndexForward":oldest}
    #     if count > 0:
    #         kwargs["Limit"] = count
    #     hash_key_condition = DDBKey("source").eq(source)
    #     if start or end:
    #         start = start if start else 0
    #         end = end if end else time.time()
    #         range_key_condition = DDBKey("timestamp").between(decimal.Decimal(start), decimal.Decimal(end))
    #         kwargs["KeyConditionExpression"] = hash_key_condition & range_key_condition
    #     else:
    #         kwargs["KeyConditionExpression"] = hash_key_condition
    #     return cls.query_all(IndexName="source-timestamp-index", **kwargs)

    # @classmethod
    # def load_n_events(cls, source, n, oldest=False):
    #     return cls.load_range(source=source, count=n, oldest=oldest)

    # @classmethod
    # def all_since(cls, source, timestamp):
    #     return cls.load_range(source, start=timestamp, oldest=False)

class ForecastObject(_ForecastObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def _shared_setup(cls, forecast):
        issued = datetime.strptime(forecast.split(":Issued:")[1].split("\n")[0].strip(), "%Y %b %d %H%M %Z") # 2023 Apr 30 2205 UTC
        timestamp = ensure_ddbsafe(issued.timestamp())
        product = forecast.split(":Product:")[1].split("\n")[0].strip()
        obj = cls.load(product=product, timestamp=timestamp)
        if obj:
            print(f"Forecast {product}@{issued} already added to database.")
            return obj, True
        info = cls(product=product, timestamp=timestamp, issued=issued)
        info["raw"] = forecast
        return info, False

    @classmethod
    def from_month_forecast(cls, forecast, save=False):
        info, in_db = cls._shared_setup(forecast)
        if in_db:
            return info
        lines = clean_lines(forecast)
        last_line_handled = -1
        info[RF_KEY] = {}
        info[KP_KEY] = {}
        info[AP_KEY] = {}
        for i in range(len(lines)):
            line = lines[i]
            if line.startswith("#"):
                continue
            if line.startswith(":"):
                continue
            pieces = line.split(" ")
            date = format_date(" ".join(pieces[:3]))
            info[RF_KEY][date] = int(pieces[3])
            info[AP_KEY][date] = int(pieces[4])
            info[KP_KEY][date] = int(pieces[5])
        if save:
            info.save()
        return info

    @classmethod
    def from_short_forecast(cls, forecast, save=False):
        info, in_db = cls._shared_setup(forecast)
        if in_db:
            return info
        lines = clean_lines(forecast)
        last_line_handled = -1
        days = [x.strip() for x in forecast.split("NOAA Kp index forecast")[1].split("\n")[1].strip().split("  ") if x.strip()]
        days_dt = [get_dt(info["issued"], day) for day in days]
        today = days_dt[0] - timedelta(days=1)
        yesterday = today - timedelta(days=1)
        days_s = [date_to_s(dt) for dt in days_dt]
        today_s = date_to_s(today)
        yesterday_s = date_to_s(yesterday)
        info[STORM_KEY] = {d:{} for d in days_s}
        info[KP_KEY] = {d:{} for d in days_s}
        info[AP_KEY] = {}
        for i in range(len(lines)):
            if i <= last_line_handled:
                continue
            line = lines[i]
            if line.startswith("#"):
                continue
            if line.startswith(":"):
                continue
            if line.startswith("NOAA Ap Index Forecast"):
                # https://www.ngdc.noaa.gov/stp/geomag/kp_ap.html
                info[AP_KEY][yesterday_s] = lines[i+1].split(" ")[-1]
                info[AP_KEY][today_s] = lines[i+2].split(" ")[-1]
                apf = lines[i+3].split(" ")[-1].split("-")
                for d in range(3):
                    info[AP_KEY][days_s[d]] = apf[d]
            if line.startswith("NOAA Geomagnetic Activity Probabilities"):
                active_pct = lines[i+1].split(" ")[-1].split("/")
                minor_pct = lines[i+2].split(" ")[-1].split("/")
                moderate_pct = lines[i+3].split(" ")[-1].split("/")
                extreme_pct = lines[i+4].split(" ")[-1].split("/")
                for d in range(3):
                    info[STORM_KEY][days_s[d]]["minor"] = int(active_pct[d])
                    info[STORM_KEY][days_s[d]]["moderate"] = int(minor_pct[d])
                    info[STORM_KEY][days_s[d]]["extreme"] = int(moderate_pct[d])
                    info[STORM_KEY][days_s[d]]["extreme"] = int(extreme_pct[d])
                last_line_handled = i+4
            if line.startswith("NOAA Kp index forecast"):
                for h in range(8):
                    kp_line = lines[i+2+h]
                    pieces = [x.strip() for x in kp_line.split(" ") if x.strip()]
                    window = pieces[0]
                    by_day = pieces[1:]
                    for d in range(3):
                        info[KP_KEY][days_s[d]][window] = float(by_day[d])
                last_line_handled = i+9

        if save:
            info.save()
        return info
