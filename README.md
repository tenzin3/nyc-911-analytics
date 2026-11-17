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
| `objectid` | Row ID (empty in the rows checked so far) |
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
│   └── download_nypd_cfs.sh   # downloads 2023-2024 month by month
├── data/                      # not committed (see .gitignore)
│   └── raw/                   # cfs_YYYY_MM.csv + download_log.txt
└── README.md
```

To come: `profiling/` and `cleaning/` (MapReduce jobs), HDFS ingestion commands, and the checkpoint report.

## Download the data

```bash
cd nyc-911-analytics
caffeinate -i bash ingestion/download_nypd_cfs.sh   # caffeinate keeps the Mac awake
```

The script downloads one month at a time through the NYC Open Data API into `data/raw/cfs_YYYY_MM.csv`. Before each month it asks the portal how many rows to expect, and a month is accepted only when the file matches; short or failed downloads are retried up to three times. It is safe to re-run: finished months are skipped.

Optional, to avoid API throttling: create a free app token on NYC Open Data and run `export SOCRATA_APP_TOKEN=your_token` first.

The data folder is in `.gitignore`: the files are too large for GitHub, and the assignment asks for a snippet rather than the whole dataset.

## Status

- [x] Find the dataset
- [x] Download 2023-2024 (14,236,236 rows)
- [ ] Load into HDFS on Dataproc
- [ ] MapReduce profiling job
- [ ] MapReduce cleaning job
- [ ] Checkpoint report (due Nov 20, 2026)
