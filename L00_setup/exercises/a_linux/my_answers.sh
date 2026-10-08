#!/usr/bin/env bash
# L00 exercise (a) - Linux CLI. Write ONE command (pipeline) per task, then run:  bash a_linux/my_answers.sh
# Run from labs/L00_setup/exercises. Solution: solutions/a_linux.sh
set -u
F=data/taxi_sample.csv                                  # 100,000 trips + header (README step 0)
Z=../../datasets/data/taxi/taxi_zone_lookup.csv         # LocationID,Borough,Zone,service_zone

echo "A1. number of data rows (without the header)";              # TODO
echo "A2. the header as one column name per line, numbered";      # TODO  (hint: head, tr, nl / cat -n)
echo "A3. trips per payment_type (column 10), most frequent first"; # TODO (cut, sort, uniq -c, sort -rn)
echo "A4. top 5 pickup zone ids (column 8)";                     # TODO
echo "A5. average fare_amount (col 11) and total revenue (col 17)"; # TODO (awk)
echo "A6. trips longer than 20 miles (column 5)";                # TODO (awk filter + wc -l)
echo "A7. trips with store_and_fwd_flag = Y";                    # TODO (grep -c)
echo "A8. trips per pickup hour, top 3 hours";                   # TODO (cut -c on column 2)
echo "A9. top 5 pickup ZONE NAMES (join with the lookup file)";  # TODO (awk with two files: NR==FNR)
echo "A10. rows with fare_amount <= 0 into data/bad_fares.csv (keep the header), count them" # TODO
