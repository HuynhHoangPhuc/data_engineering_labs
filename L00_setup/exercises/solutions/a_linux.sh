#!/usr/bin/env bash
# L00 exercise (a) - SOLUTION. Run from labs/L00_setup/exercises:  bash solutions/a_linux.sh
# Works with GNU (Linux/WSL) and BSD (macOS) tools.
set -euo pipefail
F=data/taxi_sample.csv
Z=../../datasets/data/taxi/taxi_zone_lookup.csv

echo "A1. number of data rows"
tail -n +2 "$F" | wc -l

echo "A2. header, one column per line"
head -1 "$F" | tr ',' '\n' | tr -d '"' | nl | head -20

echo "A3. trips per payment_type"
tail -n +2 "$F" | cut -d, -f10 | sort | uniq -c | sort -rn

echo "A4. top 5 pickup zone ids"
tail -n +2 "$F" | cut -d, -f8 | sort | uniq -c | sort -rn | head -5

echo "A5. average fare and total revenue"
awk -F, 'NR > 1 { fare += $11; rev += $17; n++ } END { printf "avg_fare=%.2f revenue=%.2f trips=%d\n", fare/n, rev, n }' "$F"

echo "A6. trips longer than 20 miles"
awk -F, 'NR > 1 && $5 > 20' "$F" | wc -l

echo "A7. store_and_fwd_flag = Y"
grep -c ',Y,' "$F"

echo "A8. top 3 pickup hours"
tail -n +2 "$F" | cut -d, -f2 | cut -c12-13 | sort | uniq -c | sort -rn | head -3

echo "A9. top 5 pickup zone names"
awk -F, 'NR == FNR { gsub(/"/, ""); zone[$1] = $3; next }      # 1st file: build a lookup table
         FNR > 1   { count[zone[$8]]++ }                         # 2nd file: count trips per zone name
         END       { for (z in count) print count[z] "\t" z }' "$Z" "$F" | sort -rn | head -5

echo "A10. bad fares"
{ head -1 "$F"; awk -F, 'NR > 1 && $11 <= 0' "$F"; } > data/bad_fares.csv
echo "$(($(wc -l < data/bad_fares.csv) - 1)) rows written to data/bad_fares.csv"
