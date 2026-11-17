#!/usr/bin/env python3
"""
Cleaning mapper for NYPD Calls for Service (Historic), 2023-2024.

For every raw CSV row:
  - drops rows that can't be used: wrong number of columns, missing or
    non-numeric event ID, missing or invalid incident date or time,
    incident date outside 2023-2024, or no real borough ("(null)")
  - drops the columns objectid (always empty), geo_cd_x, geo_cd_y and
    location (repeat latitude/longitude), and create_date
    (incident_date is used instead)
  - normalizes text to uppercase and timestamps to "YYYY-MM-DD HH:MM:SS"
  - combines incident_date and incident_time into one timestamp and
    extracts the hour, date, day of week and month
  - flags officer-initiated entries (radio codes starting with "75")

Output: <month code> TAB <cad_evnt_id> TAB <add_ts sort key> TAB <cleaned fields>
The reducer keeps one row per cad_evnt_id (the one added first).

Monthly output files: the job runs 24 reducers, one per month. The month
code is a single character chosen so that Hadoop's KeyFieldBasedPartitioner,
which partitions on the first key field, sends month k (0 = 2023-01,
23 = 2024-12) to reducer k: for a one-byte field the partitioner's hash is
the byte value, and the partition is (byte value) mod 24. So month k uses
the character chr(72 + k), "H" for 2023-01 up to "_" for 2024-12, and
reducer k writes the file for month k. All copies of an event fall in the
same month (checked on the full data), so duplicates still meet.

Counts of rows read, dropped and changed are reported as Hadoop counters
in the group "NYPD Cleaning".
"""
import csv
import re
import sys
from collections import defaultdict
from datetime import date

COLUMNS = [
    "objectid", "cad_evnt_id", "create_date", "incident_date", "incident_time",
    "nypd_pct_cd", "boro_nm", "patrl_boro_nm", "geo_cd_x", "geo_cd_y",
    "radio_code", "typ_desc", "cip_jobs", "add_ts", "disp_ts", "arrivd_ts",
    "closng_ts", "latitude", "longitude", "location",
]
I = {name: i for i, name in enumerate(COLUMNS)}

BOROUGHS = {"BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"}
DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T00:00:00\.000$")
TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d:[0-5]\d$")
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})\.\d{3}$")
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday"]
NUM_MONTHS = 24          # 2023-01 ... 2024-12, one reducer each


def month_code(year, month):
    """One character whose byte value mod 24 is the month's index."""
    k = (year - 2023) * 12 + (month - 1)
    return chr(72 + k)   # 72 is a multiple of 24

counters = defaultdict(int)


def text(value):
    """Trim, uppercase, and keep the output delimiters out of the field."""
    value = value.strip().upper()
    if "," in value or "\t" in value:
        counters["changed: comma or tab replaced in text"] += 1
        value = value.replace(",", ";").replace("\t", " ")
    return value


def timestamp(value):
    """2023-01-02T05:01:47.000 -> 2023-01-02 05:01:47 (empty stays empty)."""
    m = TS_RE.match(value)
    return f"{m.group(1)} {m.group(2)}" if m else ""


def clean(row):
    """Return (month_code, event_id, sort_key, cleaned_fields) or None to drop the row."""
    if len(row) != len(COLUMNS):
        counters["dropped: wrong column count"] += 1
        return None

    eid = row[I["cad_evnt_id"]].strip()
    if not eid.isdigit():
        counters["dropped: missing or non-numeric event ID"] += 1
        return None

    d = DATE_RE.match(row[I["incident_date"]])
    t = row[I["incident_time"]].strip()
    if not d or not TIME_RE.match(t):
        counters["dropped: invalid incident date or time"] += 1
        return None
    year, month, day = (int(x) for x in d.groups())
    try:
        day_of_week = DAYS[date(year, month, day).weekday()]
    except ValueError:
        counters["dropped: invalid incident date or time"] += 1
        return None
    if year not in (2023, 2024):
        counters["dropped: incident date outside 2023-2024"] += 1
        return None

    borough = text(row[I["boro_nm"]])
    if borough not in BOROUGHS:
        counters["dropped: no valid borough"] += 1
        return None

    incident_date = f"{year:04d}-{month:02d}-{day:02d}"
    radio_code = text(row[I["radio_code"]])

    precinct = row[I["nypd_pct_cd"]].strip()
    if not precinct.isdigit() or int(precinct) == 0:
        if precinct:
            counters["changed: precinct 0 set to empty"] += 1
        precinct = ""

    priority = text(row[I["cip_jobs"]])
    if priority != row[I["cip_jobs"]]:
        counters["changed: priority uppercased"] += 1

    add_ts = timestamp(row[I["add_ts"]])
    fields = [
        eid,                                   # cad_evnt_id
        f"{incident_date} {t}",                # incident_ts
        f"{incident_date} {t[:2]}:00:00",      # incident_hour (join key)
        incident_date,                         # incident_date
        str(int(t[:2])),                       # hour_of_day 0-23
        day_of_week,                           # day_of_week
        incident_date[:7],                     # month
        borough,                               # borough
        precinct,                              # precinct
        text(row[I["patrl_boro_nm"]]),         # patrol_borough
        radio_code,                            # radio_code
        text(row[I["typ_desc"]]),              # call_type
        priority,                              # priority
        "1" if radio_code.startswith("75") else "0",  # is_officer_initiated
        add_ts,                                # add_ts
        timestamp(row[I["disp_ts"]]),          # disp_ts
        timestamp(row[I["arrivd_ts"]]),        # arrivd_ts
        timestamp(row[I["closng_ts"]]),        # closng_ts
        row[I["latitude"]].strip(),            # latitude
        row[I["longitude"]].strip(),           # longitude
    ]
    # Rows without add_ts sort last, so a copy with add_ts is kept first.
    sort_key = add_ts if add_ts else "9999-99-99 99:99:99"
    return month_code(year, month), eid, sort_key, fields


def main():
    out = sys.stdout
    for row in csv.reader(sys.stdin):
        if not row:
            continue
        if row[0] == "objectid":               # header line of each file
            counters["skipped: header lines"] += 1
            continue
        counters["rows read"] += 1
        result = clean(row)
        if result is None:
            continue
        code, eid, sort_key, fields = result
        counters["rows passed to reducer"] += 1
        out.write(f"{code}\t{eid}\t{sort_key}\t{','.join(fields)}\n")

    for name, n in counters.items():
        sys.stderr.write(f"reporter:counter:NYPD Cleaning,{name},{n}\n")


if __name__ == "__main__":
    main()
