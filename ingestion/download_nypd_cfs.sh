#!/bin/bash
# download_nypd_cfs.sh
# Downloads NYPD Calls for Service (Historic) from NYC Open Data, one month
# at a time, for 2023-01-01 through 2024-12-31.
#
#   Dataset: https://data.cityofnewyork.us/d/d6zx-ckhd
#   Output:  data/raw/cfs_YYYY_MM.csv (24 files, each with a header line)
#   Log:     data/raw/download_log.txt (expected vs. downloaded rows per month)
#
# Usage:   bash ingestion/download_nypd_cfs.sh
#
# Safe to re-run: months that already downloaded and passed the row-count
# check are skipped, so after a failure just run it again.
#
# Optional: a free NYC Open Data app token avoids throttling.
#   export SOCRATA_APP_TOKEN=your_token

BASE="https://data.cityofnewyork.us/resource/d6zx-ckhd.csv"
START_YEAR=2023
END_YEAR=2024
MAX_TRIES=3

# Always work from the repo root, wherever the script is called from.
cd "$(dirname "$0")/.." || exit 1
OUT_DIR="data/raw"
LOG="$OUT_DIR/download_log.txt"
mkdir -p "$OUT_DIR"

# Calls the API. Args: output file, then extra curl arguments.
fetch() {
  local out="$1"; shift
  if [ -n "$SOCRATA_APP_TOKEN" ]; then
    curl --fail -sS --retry 3 --retry-delay 5 -G "$BASE" \
      -H "X-App-Token: $SOCRATA_APP_TOKEN" "$@" -o "$out"
  else
    curl --fail -sS --retry 3 --retry-delay 5 -G "$BASE" "$@" -o "$out"
  fi
}

total_expected=0
total_rows=0
failed=""

for year in $(seq "$START_YEAR" "$END_YEAR"); do
  for month in 1 2 3 4 5 6 7 8 9 10 11 12; do
    mm=$(printf "%02d" "$month")
    if [ "$month" -eq 12 ]; then
      next_year=$((year + 1)); next_mm="01"
    else
      next_year=$year; next_mm=$(printf "%02d" $((month + 1)))
    fi
    where="incident_date >= '${year}-${mm}-01T00:00:00' AND incident_date < '${next_year}-${next_mm}-01T00:00:00'"
    final="$OUT_DIR/cfs_${year}_${mm}.csv"
    part="$final.part"
    label="${year}-${mm}"

    # Ask the portal how many rows this month should have.
    expected=""
    if fetch "$OUT_DIR/.count.tmp" --data-urlencode "\$select=count(*)" \
         --data-urlencode "\$where=$where"; then
      expected=$(tail -1 "$OUT_DIR/.count.tmp" | tr -d '"\r ')
    fi
    case "$expected" in
      ''|*[!0-9]*)
        echo "$label: could not get the expected row count, skipping for now"
        failed="$failed $label"
        continue ;;
    esac
    total_expected=$((total_expected + expected))

    # Skip months that are already complete.
    if [ -f "$final" ]; then
      rows=$(( $(awk 'END{print NR}' "$final") - 1 ))
      if [ "$rows" -ge "$expected" ]; then
        echo "$label: already downloaded ($rows rows), skipping"
        total_rows=$((total_rows + rows))
        continue
      fi
      echo "$label: existing file is incomplete ($rows of $expected rows), downloading again"
    fi

    ok=0
    try=1
    while [ "$try" -le "$MAX_TRIES" ]; do
      echo "$label: downloading $expected rows (attempt $try of $MAX_TRIES)..."
      if fetch "$part" --data-urlencode "\$where=$where" \
           --data-urlencode "\$order=:id" \
           --data-urlencode "\$limit=5000000"; then
        rows=$(( $(awk 'END{print NR}' "$part") - 1 ))
        if [ "$rows" -ge "$expected" ]; then
          mv -f "$part" "$final"
          ok=1
          break
        fi
        echo "$label: got $rows of $expected rows, retrying"
      else
        echo "$label: download failed, retrying"
      fi
      try=$((try + 1))
      sleep 5
    done

    if [ "$ok" -eq 1 ]; then
      size=$(du -h "$final" | cut -f1)
      echo "$label: done, $rows rows, $size"
      echo "$label expected=$expected downloaded=$rows file=$final" >> "$LOG"
      total_rows=$((total_rows + rows))
    else
      echo "$label: FAILED after $MAX_TRIES attempts"
      failed="$failed $label"
    fi
  done
done

rm -f "$OUT_DIR/.count.tmp"

echo
echo "Expected rows:   $total_expected"
echo "Downloaded rows: $total_rows"
if [ -n "$failed" ]; then
  echo "Months that failed:$failed"
  echo "Run the script again to retry just those months."
  exit 1
fi
echo "All months downloaded to $OUT_DIR/"
