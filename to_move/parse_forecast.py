#!/usr/bin/env python3

from datetime import datetime
import json
import requests

FORECAST_URL = "https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt"

EASY_PREFIXES = [":Product: ",":Issued: ","Observed","Estimated","Predicted",]
STORM_KEY = "storm_forecasts"
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

t = requests.get(FORECAST_URL).text.replace("\r","")

days = [x.strip() for x in t.split("NOAA Kp index forecast")[1].split("\n")[1].strip().split("  ") if x.strip()]
issued = datetime.strptime(t.split(":Issued:")[1].split("\n")[0].strip(), "%Y %b %d %H%M %Z") # 2023 Apr 30 2205 UTC
days_dt = [get_dt(issued, day) for day in days]
days_s = [date_to_s(dt) for dt in days_dt]
for day in days:
    print(get_dt(issued, day))

info = {"raw":t}
lines = clean_lines(t)
last_line_handled = -1
info[STORM_KEY] = {d:{} for d in days_s}
info[KP_KEY] = {d:{} for d in days_s}
for i in range(len(lines)):
    if i <= last_line_handled:
        continue
    line = lines[i]
    for prefix in EASY_PREFIXES:
        if line.startswith(prefix):
            info[clean_key(prefix)] = line[len(prefix):].strip()
            if info[clean_key(prefix)].startswith("Ap "):
                # I'm not positive, but I think this stands for "approximately" or something like that.
                info[clean_key(prefix)] = info[clean_key(prefix)][3:]
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

print(json.dumps(info, indent=2, sort_keys=True))
