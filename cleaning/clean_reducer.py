#!/usr/bin/env python3
"""
Cleaning reducer for NYPD Calls for Service (Historic), 2023-2024.

Input lines arrive grouped by (month code, cad_evnt_id):
    <month code> TAB <cad_evnt_id> TAB <add_ts sort key> TAB <cleaned fields>

Each reducer receives exactly one month (see clean_mapper.py), so its
output file holds that month's calls.

Some events appear more than once (the same call dispatched or closed more
than once). For each event ID, keep only the row that was added first
(earliest add_ts), so each call is counted once.

Output: <cad_evnt_id> TAB <remaining cleaned fields>. The job sets the
output separator to a comma, so each line is written as plain CSV.

As a check, the reducer reports which month(s) it saw; more than one month
in a reducer means the monthly split went wrong, and run_clean.sh stops.
"""
import sys

MONTH = 6                 # position of "month" in the cleaned fields

kept = 0
removed = 0
months_seen = set()


def emit(fields):
    global kept
    kept += 1
    eid, rest = fields.split(",", 1)
    months_seen.add(fields.split(",", MONTH + 1)[MONTH])
    print(f"{eid}\t{rest}")


def main():
    global removed
    current, best, copies = None, None, 0
    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 4:
            continue
        _code, eid, sort_key, fields = parts
        if eid != current:
            if current is not None:
                emit(best[1])
                removed += copies - 1
            current, best, copies = eid, (sort_key, fields), 0
        copies += 1
        # Earliest add_ts wins; ties are broken by the row text so the
        # result is the same on every run.
        if (sort_key, fields) < best:
            best = (sort_key, fields)
    if current is not None:
        emit(best[1])
        removed += copies - 1

    err = sys.stderr
    err.write(f"reporter:counter:NYPD Cleaning,dropped: duplicate event ID,{removed}\n")
    err.write(f"reporter:counter:NYPD Cleaning,rows written,{kept}\n")
    for m in months_seen:
        err.write(f"reporter:counter:NYPD Rows per month,{m},{kept if len(months_seen) == 1 else 0}\n")
    if len(months_seen) > 1:
        err.write("reporter:counter:NYPD Cleaning,ERROR: reducer received more than one month,1\n")


if __name__ == "__main__":
    main()
