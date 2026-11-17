#!/bin/bash
# run_clean.sh
# Runs the cleaning MapReduce job (Hadoop Streaming, Python) on NYU Dataproc.
#
# Usage, on the cluster, from the folder holding the three cleaning files:
#   bash run_clean.sh
#
# Input:  /user/tt3124_nyu_edu/nypd_cfs_raw     (24 raw monthly CSVs, unchanged)
# Output: /user/tt3124_nyu_edu/nypd_cfs_clean   (one cleaned row per call,
#                                                24 gzipped CSV files, one per month:
#                                                cfs_clean_2023_01.csv.gz ... cfs_clean_2024_12.csv.gz)
#
# Local files written next to this script:
#   clean_job.log       full Hadoop output of the job
#   clean_summary.txt   rows read, dropped and written (job counters)

cd "$(dirname "$0")" || exit 1

RAW=/user/tt3124_nyu_edu/nypd_cfs_raw
CLEAN=/user/tt3124_nyu_edu/nypd_cfs_clean

# Find the Hadoop Streaming jar.
JAR=$(ls /usr/lib/hadoop/hadoop-streaming*.jar /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
  JAR=$(find / -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)
fi
if [ -z "$JAR" ]; then
  echo "Could not find the hadoop-streaming jar" >&2
  exit 1
fi
echo "Using $JAR"

# Remove output from an earlier run (Hadoop refuses to overwrite it).
hdfs dfs -rm -r -f "$CLEAN"

# Key = (month code, cad_evnt_id). Partitioning on the month code alone
# (-k1,1) sends each month to its own reducer, one of 24, and all copies
# of an event to the same reducer (see clean_mapper.py).
# The comma output separator makes each output line plain CSV.
# Output is gzipped (about 7x smaller); Hive and Hadoop read .gz directly.
hadoop jar "$JAR" \
  -D mapreduce.job.name="nypd_cfs_clean" \
  -D mapreduce.job.reduces=24 \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -D mapreduce.output.textoutputformat.separator=, \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
  -files clean_mapper.py,clean_reducer.py \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -mapper "python3 clean_mapper.py" \
  -reducer "python3 clean_reducer.py" \
  -input "$RAW" \
  -output "$CLEAN" 2>&1 | tee clean_job.log

if ! hdfs dfs -test -e "$CLEAN/_SUCCESS"; then
  echo "Cleaning job failed; see clean_job.log" >&2
  exit 1
fi

# Check the monthly split before renaming: no reducer may have received
# more than one month.
if grep -q "ERROR: reducer received more than one month" clean_job.log; then
  echo "A reducer received more than one month; files not renamed. See clean_job.log" >&2
  exit 1
fi

# Rename Hadoop's default output names to monthly names:
# part-00000.gz -> cfs_clean_2023_01.csv.gz ... part-00023.gz -> cfs_clean_2024_12.csv.gz
# and check that each file's first row is from the month in its new name.
for k in $(seq 0 23); do
  year=$((2023 + k / 12))
  month=$(printf '%02d' $((k % 12 + 1)))
  src="$CLEAN/part-$(printf '%05d' "$k").gz"
  dst="$CLEAN/cfs_clean_${year}_${month}.csv.gz"
  first_month=$(hdfs dfs -text "$src" 2>/dev/null | head -1 | cut -d, -f7)
  if [ "$first_month" != "$year-$month" ]; then
    echo "$src holds $first_month, expected $year-$month; stopping" >&2
    exit 1
  fi
  hdfs dfs -mv "$src" "$dst"
done
hdfs dfs -ls -h "$CLEAN"

# Summary: the custom counter group "NYPD Cleaning" from the job log,
# plus the output size and a few sample rows.
{
  echo "== Cleaning job counters =="
  awk '/^\t[^\t]/ { g = ($0 ~ /NYPD/) } g' clean_job.log
  echo
  echo "== Output size =="
  hdfs dfs -du -s -h "$CLEAN"
  echo
  echo "== First 5 cleaned rows =="
  hdfs dfs -text "$CLEAN"/cfs_clean_2023_01.csv.gz | head -5
} > clean_summary.txt

echo
cat clean_summary.txt
