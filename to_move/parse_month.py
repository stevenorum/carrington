#!/usr/bin/env python3

from datetime import datetime, timedelta
import json
import requests

FORECAST_URL = "https://services.swpc.noaa.gov/text/27-day-outlook.txt"

EASY_PREFIXES = [":Product: ",":Issued: ",]
RF_KEY = "rf_forecasts"
AP_KEY= "ap_forecasts"
KP_KEY = "kp_forecasts"

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

def format_date(s):
    # Input: 2023 May 01
    # Output: 2023/05/01
    return datetime.strptime(s,"%Y %b %d").strftime("%Y/%m/%d")

t = requests.get(FORECAST_URL).text.replace("\r","")

issued = datetime.strptime(t.split(":Issued:")[1].split("\n")[0].strip(), "%Y %b %d %H%M %Z") # 2023 Apr 30 2205 UTC

info = {"raw":t}
lines = clean_lines(t)
last_line_handled = -1
info[RF_KEY] = {}
info[KP_KEY] = {}
info[AP_KEY] = {}
for i in range(len(lines)):
    line = lines[i]
    if line.startswith("#"):
        continue
    for prefix in EASY_PREFIXES:
        if line.startswith(prefix):
            info[clean_key(prefix)] = line[len(prefix):].strip()
    if line.startswith(":"):
        # These were all handled in the EASY_PREFIXES section
        continue
    pieces = line.split(" ")
    date = format_date(" ".join(pieces[:3]))
    info[RF_KEY][date] = int(pieces[3])
    info[AP_KEY][date] = int(pieces[4])
    info[KP_KEY][date] = int(pieces[5])

print(json.dumps(info, indent=2, sort_keys=True))
