# nyc-911-analytics

My data source for the Group 3 project in CSCI-GA.2436 Real-time and Big Data Analytics (NYU, Fall 2026): **NYPD Calls for Service**, cleaned with MapReduce and aggregated to a borough × hour key so it can be joined with the team's weather and transportation data.

**Team project:** *Weather, 911 Calls and Transportation in NYC*, asking how weather conditions affect the way New Yorkers travel and how often they call 911.

| Dataset | Owner |
|---|---|
| NYPD Calls for Service (Historic) | Tenzin Tsundue (this repo) |
| NOAA hourly weather (ISD) | Naveen Selvan Deepa |
| MTA/PATH subway hourly ridership | Yuhao Chen |
| Citi Bike trip histories | Shipra Anil Mathur |
| TLC yellow and green taxi trips | To be determined |

## Data source

| | |
|---|---|
| Dataset | NYPD Calls for Service (Historic) |
| Publisher | NYPD, via NYC Open Data |
| Dataset ID | `d6zx-ckhd` |
| Page | https://data.cityofnewyork.us/d/d6zx-ckhd |
| Data dictionary | `NYPD_CallsForService_DataDictionary.xlsx` (Attachments on the dataset page) |
| Full dataset | 54.9M rows, incident dates 2018-01-01 to 2025-12-31 |
| Used here | 2023-01-01 to 2024-12-31, filtered on `incident_date` |

Each row is one entry in ICAD, the NYPD's computer-aided dispatch system. Entries include calls from the public and entries started by officers themselves (for example patrols and station inspections).

### Downloaded data

| Year | Rows | Size |
|---|---|---|
| 2023 | 7,050,129 | 2.6 GB |
| 2024 | 7,186,107 | 2.4 GB |
| **Total** | **14,236,236** | **4.9 GB** |

Every month's row count matched the count the portal reported for that month (see `data/raw/download_log.txt`).

### Columns

The CSV has 20 columns:

| Column | Description |
|---|---|
| `objectid` | Row ID (empty in every row) |
| `cad_evnt_id` | ICAD event ID |
| `create_date` | Date the entry was created |
| `incident_date` | Date of the incident |
| `incident_time` | Time of the incident (`HH:MM:SS`) |
| `nypd_pct_cd` | NYPD precinct |
| `boro_nm` | Borough |
| `patrl_boro_nm` | Patrol borough |
| `geo_cd_x`, `geo_cd_y` | State plane coordinates |
| `radio_code` | Radio code of the call type |
| `typ_desc` | Description of the call type |
| `cip_jobs` | Priority: Non CIP, Non Critical, Serious, Critical |
| `add_ts` | Time the entry was added |
| `disp_ts` | Time a unit was dispatched |
| `arrivd_ts` | Time a unit arrived |
| `closng_ts` | Time the entry was closed |
| `latitude`, `longitude` | Location |
| `location` | Location as WKT `POINT (lon lat)` |

Sample row:

```csv
"objectid","cad_evnt_id","create_date","incident_date","incident_time","nypd_pct_cd","boro_nm","patrl_boro_nm","geo_cd_x","geo_cd_y","radio_code","typ_desc","cip_jobs","add_ts","disp_ts","arrivd_ts","closng_ts","latitude","longitude","location"
,"91276372","2023-01-02T00:00:00.000","2023-01-02T00:00:00.000","05:00:55","40","BRONX","PATROL BORO BRONX","1007976","232906","11C4","ALARMS: COMMERCIAL/BURGLARY","Non CIP","2023-01-02T05:01:47.000","2023-01-02T05:05:49.000","2023-01-02T05:16:06.000","2023-01-02T05:33:53.000","40.805925436000052","-73.914298569999971","POINT (-73.91429856999997 40.80592543600005)"
```

Every field is quoted, and `typ_desc` can contain commas, so the MapReduce jobs need a quote-aware CSV parser rather than a plain split on commas.

## Repository layout

```
nyc-911-analytics/
├── ingestion/
│   └── download_nypd_cfs.sh     # downloads 2023-2024 month by month
├── profiling/
│   ├── profile_mapper.py        # MapReduce profiling job (Hadoop Streaming)
│   ├── profile_reducer.py
│   └── run_profile.sh           # submits the job on Dataproc
├── results/
│   └── profiling/
│       └── profile_results.tsv  # output of the profiling job
├── screenshots/                 # evidence of each step, run on Dataproc
│   ├── ingestion/
│   └── profiling/
├── data/
│   ├── lfs/                     # the 24 CSVs gzipped (847 MB), in Git LFS
│   └── raw/                     # the 24 CSVs uncompressed (4.9 GB), not committed
└── README.md
```

## 1. Download the data

```bash
cd nyc-911-analytics
caffeinate -i bash ingestion/download_nypd_cfs.sh   # caffeinate keeps the Mac awake
```

The script downloads one month at a time through the NYC Open Data API into `data/raw/cfs_YYYY_MM.csv`. Before each month it asks the portal how many rows to expect, and a month is accepted only when the file matches; short or failed downloads are retried up to three times. It is safe to re-run: finished months are skipped.

Optional, to avoid API throttling: create a free app token on NYC Open Data and run `export SOCRATA_APP_TOKEN=your_token` first.

The uncompressed files are too large for GitHub (each month is about 200 MB, over GitHub's 100 MB file limit), so `data/raw/` is not committed. A gzipped copy of each month (about 35 MB each, 847 MB in total) is kept in `data/lfs/` through Git LFS. To clone only the code without downloading the data:

```bash
GIT_LFS_SKIP_SMUDGE=1 git clone https://github.com/tenzin3/nyc-911-analytics.git
```

## 2. Load into HDFS

1. Upload the 24 CSVs to the NYU Data Ingest Console (https://dataproc.hpc.nyu.edu/ingest), into the folder `tt3124_nypd_cfs` in the `nyu-dataproc-hdfs-ingest` bucket. Files there are deleted after 2 days.
2. On the cluster (https://dataproc.hpc.nyu.edu/ssh), copy them into HDFS and check:

```bash
hadoop distcp gs://nyu-dataproc-hdfs-ingest/tt3124_nypd_cfs /user/tt3124_nyu_edu/nypd_cfs_raw
hdfs dfs -ls -h /user/tt3124_nyu_edu/nypd_cfs_raw
hdfs dfs -du -s -h /user/tt3124_nyu_edu/nypd_cfs_raw
```

Result: 24 files, 5,126,454,731 bytes copied out of 5,126,454,731 expected (4.8 GB as HDFS reports it). See `screenshots/ingestion/`.

## 3. Profile the data (MapReduce)

The profiling job is written in Python for Hadoop Streaming. On the cluster, upload the three files in `profiling/` into one folder and run:

```bash
bash run_profile.sh
```

It reads `/user/tt3124_nyu_edu/nypd_cfs_raw`, writes to `/user/tt3124_nyu_edu/nypd_cfs_profile` in HDFS, and saves a local copy as `profile_results.tsv` (kept in this repo under `results/profiling/`).

How it works:

- **Mapper:** parses each row with a quote-aware CSV reader and checks every column: empty values, badly formatted dates, times and timestamps, values out of range, timestamps out of order, text needing normalization, and how often each borough, precinct, radio code, call type and priority occurs. It adds up these counts in memory and emits the totals once at the end (in-mapper combining), so the shuffle stays small. It also emits every event ID so duplicates can be found.
- **Reducer:** sums the counts, takes minimums and maximums, and counts how often each event ID occurs. It runs as a single reducer so the duplicate summary covers all rows.

Run on the full dataset: 24 input files, 48 splits, 50 map tasks and 1 reduce task, finished in under two minutes. Output: 1,251 lines. See `screenshots/profiling/`.

### Profiling results

All counts are out of **14,236,236 rows**.

**Clean, no action needed:**

| Check | Result |
|---|---|
| Rows with the wrong number of columns | 0 |
| Badly formatted `incident_date`, `incident_time`, `create_date` or any timestamp | 0 |
| Incident dates outside 2023-2024 | 0 (range 2023-01-01 to 2024-12-31) |
| Missing or non-numeric `cad_evnt_id` | 0 |
| Missing `incident_date`, `incident_time`, `boro_nm`, `radio_code`, `typ_desc`, `cip_jobs` | 0 |
| Missing or non-numeric latitude, longitude or state plane coordinates | 0 |
| Locations outside NYC | 0 (latitude 40.4986 to 40.9141, longitude -74.2547 to -73.7003) |
| Text fields with leading or trailing spaces | 0 |
| Rows per month | Matches the download for all 24 months |

**Problems found:**

| Finding | Rows | Share |
|---|---|---|
| `objectid` is empty | 14,236,236 | 100% |
| Event IDs that appear more than once | 441,862 IDs, 448,212 extra rows | 3.15% |
| No arrival time (`arrivd_ts` empty) | 3,013,829 | 21.2% |
| `create_date` differs from `incident_date` | 1,944 | 0.01% |
| Closed before added (`closng_ts` < `add_ts`) | 388 | <0.01% |
| Borough and patrol borough are the text `(null)` | 304 | <0.01% |
| Precinct is 0 | 298 | <0.01% |
| Arrived before dispatched (`arrivd_ts` < `disp_ts`) | 83 | <0.01% |
| Dispatched before added (`disp_ts` < `add_ts`) | 79 | <0.01% |
| No closing time (`closng_ts` empty) | 71 | <0.01% |
| Precinct empty | 6 | <0.01% |
| No dispatch time (`disp_ts` empty) | 1 | <0.01% |
| `cip_jobs` in mixed case ("Non CIP", "Critical") | all rows | 100% |

There are 13,788,024 distinct event IDs; with the 448,212 extra rows from duplicates, that accounts for all 14,236,236 rows.

### Observations

- **Duplicate event IDs are not copies.** The repeated rows share the event, location, call type and incident time, but have different dispatch, arrival or closing times. They look like the same call being dispatched or closed more than once, so for counting calls each event ID should count once.
- **37.6% of entries are not calls from the public.** 5,347,737 rows have radio codes starting with "75", which by their descriptions are entries officers create themselves: patrols, inspections and visits. The largest are "VISIBILITY PATROL: DIRECTED" (1,555,781), "STATION INSPECTION BY TRANSIT BUREAU PERSONNEL" (1,310,865), "TRAIN RUN/MOBILE ORDER MAINTENANCE SWEEP" (591,751) and "TRANSIT PATROL/INSPECTION BY NON-TRANSIT BUREAU PERSONNEL" (523,554). Patrol schedules don't respond to weather the way public calls do, so these need to be separated before measuring weather effects on 911 volume. (To be confirmed against the data dictionary.)
- **Missing arrival times are expected.** 21.2% of entries have no arrival time, which fits entries where no unit was sent or arrived. The call itself still happened, so these rows stay.
- **The `(null)` boroughs are a placeholder, not an empty field.** The 304 rows with borough `(null)` mostly also have precinct 0; they can't be placed in a borough, so they can't join the borough × hour table.
- **Busiest times:** calls peak from 4 pm to 7 pm (843,294 entries in the 16:00 hour) and are lowest around 5 am (287,223 in the 05:00 hour).
- **By borough:** Brooklyn 4,326,291, Manhattan 3,981,566, Bronx 2,778,306, Queens 2,616,747, Staten Island 533,022.
- **By priority (`cip_jobs`):** Non CIP 13,205,501, Non Critical 526,953, Serious 327,899, Critical 175,883.
- **Variety:** 516 distinct radio codes, 512 distinct call types, and 78 precincts (plus the 0 and empty values above).
- **Timestamps run past midnight on Dec 31, 2024** (latest `add_ts` 2025-01-01 00:44:37) because calls from the last evening were added or closed after midnight. This is expected; the incident dates are all within 2023-2024.

### What cleaning will do

Based on these results:

- Drop `objectid` (always empty) and the duplicate location columns `geo_cd_x`, `geo_cd_y` and `location`
- Keep one row per `cad_evnt_id`
- Drop rows whose borough is `(null)`
- Normalize text to uppercase and combine `incident_date` and `incident_time` into one timestamp, with the hour extracted
- Flag officer-initiated entries (radio codes starting with "75")
- Count calls per borough per hour for the team's join

## Status

- [x] Find the dataset
- [x] Download 2023-2024 (14,236,236 rows)
- [x] Load into HDFS on Dataproc
- [x] MapReduce profiling job
- [ ] MapReduce cleaning job
- [ ] Checkpoint report (due Nov 20, 2026)
