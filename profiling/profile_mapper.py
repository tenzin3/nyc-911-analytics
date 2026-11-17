#!/usr/bin/env python3
"""
Profiling mapper for NYPD Calls for Service (Historic), 2023-2024.

Reads raw CSV rows from stdin and describes every column: empty values,
badly formatted values, value ranges and value distributions.

To keep the shuffle small, the mapper adds up its counts in memory
(in-mapper combining) and emits the totals once, at the end. The only
per-row output is the event ID, which the reducer needs to find duplicates.

Output, one record per line, tab-separated (the first 3 fields are the key):
    COUNT   <column>  <metric>  <n>      a count, summed by the reducer
    VALUE   <column>  <value>   <n>      how often a value occurs
    MINNUM  <column>  -         <x>      smallest number seen
    MAXNUM  <column>  -         <x>      largest number seen
    MINSTR  <column>  -         <s>      earliest date/timestamp seen
    MAXSTR  <column>  -         <s>      latest date/timestamp seen
    ID      cad_evnt_id  <id>   1        one per row, for duplicate detection
"""
import csv
import re
import sys
from collections import defaultdict

COLUMNS = [
    "objectid", "cad_evnt_id", "create_date", "incident_date", "incident_time",
    "nypd_pct_cd", "boro_nm", "patrl_boro_nm", "geo_cd_x", "geo_cd_y",
    "radio_code", "typ_desc", "cip_jobs", "add_ts", "disp_ts", "arrivd_ts",
    "closng_ts", "latitude", "longitude", "location",
]
I = {name: i for i, name in enumerate(COLUMNS)}

DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T00:00:00\.000$")
TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d:[0-5]\d$")
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T([01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}$")

TIMESTAMP_COLS = ["add_ts", "disp_ts", "arrivd_ts", "closng_ts"]
CATEGORICAL_COLS = ["boro_nm", "patrl_boro_nm", "nypd_pct_cd",
                    "radio_code", "typ_desc", "cip_jobs"]
TEXT_COLS = ["boro_nm", "patrl_boro_nm", "radio_code", "typ_desc", "cip_jobs"]
NUMERIC_COLS = ["nypd_pct_cd", "geo_cd_x", "geo_cd_y", "latitude", "longitude"]

# Rough bounding box around the five boroughs.
NYC_LAT = (40.47, 40.93)
NYC_LON = (-74.27, -73.68)

counts = defaultdict(int)        # (kind, column, key) -> n
num_min, num_max = {}, {}        # column -> float
str_min, str_max = {}, {}        # column -> str


def to_float(s):
    try:
        return float(s)
    except ValueError:
        return None


def track_num(col, x):
    if col not in num_min or x < num_min[col]:
        num_min[col] = x
    if col not in num_max or x > num_max[col]:
        num_max[col] = x


def track_str(col, s):
    if col not in str_min or s < str_min[col]:
        str_min[col] = s
    if col not in str_max or s > str_max[col]:
        str_max[col] = s


def profile(row):
    out = sys.stdout
    counts[("COUNT", "_rows", "total")] += 1

    # Empty values, for every column.
    for name, value in zip(COLUMNS, row):
        if value.strip() == "":
            counts[("COUNT", name, "empty")] += 1

    # cad_evnt_id: should be a number, and unique.
    eid = row[I["cad_evnt_id"]].strip()
    if eid:
        if eid.isdigit():
            out.write(f"ID\tcad_evnt_id\t{eid}\t1\n")
        else:
            counts[("COUNT", "cad_evnt_id", "not_numeric")] += 1

    # incident_date: format, range, month distribution.
    d = row[I["incident_date"]]
    m = DATE_RE.match(d)
    if d and not m:
        counts[("COUNT", "incident_date", "bad_format")] += 1
    elif m:
        track_str("incident_date", d[:10])
        counts[("VALUE", "incident_month", d[:7])] += 1
        if m.group(1) not in ("2023", "2024"):
            counts[("COUNT", "incident_date", "outside_2023_2024")] += 1

    # create_date: format, and whether it matches incident_date.
    c = row[I["create_date"]]
    if c and not DATE_RE.match(c):
        counts[("COUNT", "create_date", "bad_format")] += 1
    elif c and d and c != d:
        counts[("COUNT", "create_date", "differs_from_incident_date")] += 1

    # incident_time: HH:MM:SS, hour distribution.
    t = row[I["incident_time"]]
    if t:
        if TIME_RE.match(t):
            counts[("VALUE", "incident_hour", t[:2])] += 1
        else:
            counts[("COUNT", "incident_time", "bad_format")] += 1

    # Timestamps: format, range, and order.
    ts = {}
    for col in TIMESTAMP_COLS:
        v = row[I[col]]
        if not v:
            continue
        if TS_RE.match(v):
            ts[col] = v
            track_str(col, v)
        else:
            counts[("COUNT", col, "bad_format")] += 1
    # ISO timestamps compare correctly as strings.
    if "add_ts" in ts and "disp_ts" in ts and ts["disp_ts"] < ts["add_ts"]:
        counts[("COUNT", "timestamps", "disp_before_add")] += 1
    if "disp_ts" in ts and "arrivd_ts" in ts and ts["arrivd_ts"] < ts["disp_ts"]:
        counts[("COUNT", "timestamps", "arrivd_before_disp")] += 1
    if "add_ts" in ts and "closng_ts" in ts and ts["closng_ts"] < ts["add_ts"]:
        counts[("COUNT", "timestamps", "closng_before_add")] += 1

    # Numeric columns: parseable, range.
    nums = {}
    for col in NUMERIC_COLS:
        v = row[I[col]].strip()
        if not v:
            continue
        x = to_float(v)
        if x is None:
            counts[("COUNT", col, "not_numeric")] += 1
        else:
            nums[col] = x
            track_num(col, x)
    if "latitude" in nums and "longitude" in nums:
        lat, lon = nums["latitude"], nums["longitude"]
        if not (NYC_LAT[0] <= lat <= NYC_LAT[1] and NYC_LON[0] <= lon <= NYC_LON[1]):
            counts[("COUNT", "latitude_longitude", "outside_nyc")] += 1

    # Text columns: values that would need normalizing.
    for col in TEXT_COLS:
        v = row[I[col]]
        if v != v.strip():
            counts[("COUNT", col, "extra_whitespace")] += 1
        if v != v.upper():
            counts[("COUNT", col, "not_uppercase")] += 1

    # Distributions of categorical columns.
    for col in CATEGORICAL_COLS:
        v = row[I[col]].strip()
        counts[("VALUE", col, v if v else "(empty)")] += 1


def seed_zero_counts():
    """Start every check at 0 so checks that find nothing still show up."""
    for key in [("_rows", "total"), ("_rows", "header_lines"),
                ("_rows", "wrong_column_count"),
                ("cad_evnt_id", "not_numeric"),
                ("incident_date", "bad_format"),
                ("incident_date", "outside_2023_2024"),
                ("create_date", "bad_format"),
                ("create_date", "differs_from_incident_date"),
                ("incident_time", "bad_format"),
                ("timestamps", "disp_before_add"),
                ("timestamps", "arrivd_before_disp"),
                ("timestamps", "closng_before_add"),
                ("latitude_longitude", "outside_nyc")]:
        counts[("COUNT",) + key] = 0
    for col in COLUMNS:
        counts[("COUNT", col, "empty")] = 0
    for col in TIMESTAMP_COLS:
        counts[("COUNT", col, "bad_format")] = 0
    for col in NUMERIC_COLS:
        counts[("COUNT", col, "not_numeric")] = 0
    for col in TEXT_COLS:
        counts[("COUNT", col, "extra_whitespace")] = 0
        counts[("COUNT", col, "not_uppercase")] = 0


def main():
    seed_zero_counts()
    for row in csv.reader(sys.stdin):
        if not row:
            continue
        if row[0] == "objectid":                 # header line of each file
            counts[("COUNT", "_rows", "header_lines")] += 1
            continue
        if len(row) != len(COLUMNS):
            counts[("COUNT", "_rows", "wrong_column_count")] += 1
            continue
        profile(row)

    out = sys.stdout
    for (kind, col, key), n in counts.items():
        key = key.replace("\t", " ")
        out.write(f"{kind}\t{col}\t{key}\t{n}\n")
    for col, x in num_min.items():
        out.write(f"MINNUM\t{col}\t-\t{x!r}\n")
    for col, x in num_max.items():
        out.write(f"MAXNUM\t{col}\t-\t{x!r}\n")
    for col, s in str_min.items():
        out.write(f"MINSTR\t{col}\t-\t{s}\n")
    for col, s in str_max.items():
        out.write(f"MAXSTR\t{col}\t-\t{s}\n")


if __name__ == "__main__":
    main()
