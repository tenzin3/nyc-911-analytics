#!/usr/bin/env python3
"""
Profiling reducer for NYPD Calls for Service (Historic), 2023-2024.

Combines the mapper output. Input lines are sorted so that identical
(kind, column, key) triples arrive together:
    COUNT / VALUE      -> summed
    MINNUM / MAXNUM    -> smallest / largest number
    MINSTR / MAXSTR    -> earliest / latest date or timestamp
    ID                 -> occurrences of each event ID; only a summary
                          (distinct IDs, duplicated IDs, extra rows) is
                          written, at the end

Run with a single reducer so the duplicate-ID summary covers all rows.

Output: kind <tab> column <tab> key <tab> result
"""
import sys

ids_distinct = 0
ids_duplicated = 0
ids_extra_rows = 0
dup_examples = []


def finish(kind, col, key, values):
    global ids_distinct, ids_duplicated, ids_extra_rows
    if kind in ("COUNT", "VALUE"):
        print(f"{kind}\t{col}\t{key}\t{sum(int(v) for v in values)}")
    elif kind == "MINNUM":
        print(f"MIN\t{col}\t-\t{min(float(v) for v in values)!r}")
    elif kind == "MAXNUM":
        print(f"MAX\t{col}\t-\t{max(float(v) for v in values)!r}")
    elif kind == "MINSTR":
        print(f"MIN\t{col}\t-\t{min(values)}")
    elif kind == "MAXSTR":
        print(f"MAX\t{col}\t-\t{max(values)}")
    elif kind == "ID":
        n = sum(int(v) for v in values)
        ids_distinct += 1
        if n > 1:
            ids_duplicated += 1
            ids_extra_rows += n - 1
            if len(dup_examples) < 10:
                dup_examples.append(f"{key}x{n}")


def main():
    current, values = None, []
    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 4:
            continue
        group = tuple(parts[:3])
        if group != current:
            if current is not None:
                finish(*current, values)
            current, values = group, []
        values.append(parts[3])
    if current is not None:
        finish(*current, values)

    print(f"COUNT\tcad_evnt_id\tdistinct_ids\t{ids_distinct}")
    print(f"COUNT\tcad_evnt_id\tduplicated_ids\t{ids_duplicated}")
    print(f"COUNT\tcad_evnt_id\textra_rows_from_duplicates\t{ids_extra_rows}")
    if dup_examples:
        print(f"COUNT\tcad_evnt_id\tduplicate_examples\t{' '.join(dup_examples)}")


if __name__ == "__main__":
    main()
