#!/bin/bash
# run_profile.sh
# Runs the profiling MapReduce job (Hadoop Streaming, Python) on NYU Dataproc.
#
# Usage, on the cluster, from the folder holding the three profiling files:
#   bash run_profile.sh
#
# Input:  /user/tt3124_nyu_edu/nypd_cfs_raw      (24 raw monthly CSVs)
# Output: /user/tt3124_nyu_edu/nypd_cfs_profile  (in HDFS)
#         profile_results.tsv                    (local copy of the results)

cd "$(dirname "$0")" || exit 1

INPUT=/user/tt3124_nyu_edu/nypd_cfs_raw
OUTPUT=/user/tt3124_nyu_edu/nypd_cfs_profile

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
hdfs dfs -rm -r -f "$OUTPUT"

# The key is the first 3 tab-separated fields (kind, column, key), so that
# identical keys meet at the reducer. One reducer, so the duplicate-ID
# summary covers every row.
hadoop jar "$JAR" \
  -D mapreduce.job.name="nypd_cfs_profile" \
  -D stream.num.map.output.key.fields=3 \
  -D mapreduce.job.reduces=1 \
  -files profile_mapper.py,profile_reducer.py \
  -mapper "python3 profile_mapper.py" \
  -reducer "python3 profile_reducer.py" \
  -input "$INPUT" \
  -output "$OUTPUT" || exit 1

hdfs dfs -cat "$OUTPUT"/part-* > profile_results.tsv
echo "Results: $(wc -l < profile_results.tsv) lines in profile_results.tsv"
grep -P '^COUNT\t_rows\t' profile_results.tsv
grep -P '^COUNT\tcad_evnt_id\t' profile_results.tsv
