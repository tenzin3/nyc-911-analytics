# Screenshots

Evidence of each step, run on NYU Dataproc. Files are numbered in the order the steps were run.

## ingestion/

Copying the 24 monthly CSVs from the NYU ingest bucket into HDFS (`hadoop distcp`).

| File | Shows |
|---|---|
| `01_distcp_job_progress.png` | `hadoop distcp` from `gs://nyu-dataproc-hdfs-ingest/tt3124_nypd_cfs` to `/user/tt3124_nyu_edu/nypd_cfs_raw`: 24 splits, map 0% to 100%, job completed successfully |
| `02_distcp_job_counters.png` | Job counters: 24 files copied, 5,126,454,731 bytes copied out of 5,126,454,731 expected |
| `03_hdfs_files_verified.png` | `hdfs dfs -ls -h` listing all 24 files and `hdfs dfs -du -s -h` showing 4.8 GB in total |
