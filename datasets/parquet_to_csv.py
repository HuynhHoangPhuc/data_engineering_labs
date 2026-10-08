#!/usr/bin/env python3
"""Convert a NYC taxi Parquet file (or a sample of it) to headerless CSV.

Used by L02 (MapReduce reads text) and L03 (Hive external table over CSV).
Requires pyarrow -- it is pre-installed in the course Hadoop image
(de-labs/hadoop), so run it inside a Hadoop container, e.g.:

    docker compose exec namenode python3 /datasets/parquet_to_csv.py \
        /datasets/data/taxi/yellow_tripdata_2024-01.parquet \
        /datasets/data/taxi/csv/yellow_2024-01.csv --limit 200000

Timestamps are written as 'YYYY-MM-DD HH:MM:SS' (Hive TIMESTAMP text format).
Column order is the Parquet column order (printed with --show-schema).
"""
import argparse
import os
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("src", help="input .parquet file")
    ap.add_argument("dst", nargs="?", help="output .csv file")
    ap.add_argument("--limit", type=int, default=0, help="max rows (0 = all)")
    ap.add_argument("--header", action="store_true", help="write a header line")
    ap.add_argument("--show-schema", action="store_true", help="print the Parquet schema and exit")
    args = ap.parse_args()

    pf = pq.ParquetFile(args.src)
    if args.show_schema or not args.dst:
        print(pf.schema_arrow)
        print(f"rows: {pf.metadata.num_rows:,}  row groups: {pf.metadata.num_row_groups}")
        return 0

    os.makedirs(os.path.dirname(os.path.abspath(args.dst)), exist_ok=True)
    written = 0
    tmp = args.dst + ".part"
    with open(tmp, "wb") as sink:
        writer = None
        for batch in pf.iter_batches(batch_size=100_000):
            if args.limit and written >= args.limit:
                break
            if args.limit:
                batch = batch.slice(0, args.limit - written)
            cols, names = [], []
            for name, col in zip(batch.schema.names, batch.columns):
                if pa.types.is_timestamp(col.type):
                    # cast to whole seconds first, otherwise %S prints "55.000000"
                    col = pc.strftime(pc.cast(col, pa.timestamp("s"), safe=False), format="%Y-%m-%d %H:%M:%S")
                cols.append(col)
                names.append(name)
            table = pa.Table.from_arrays(cols, names=names)
            if writer is None:
                writer = pacsv.CSVWriter(
                    sink, table.schema,
                    write_options=pacsv.WriteOptions(include_header=args.header, quoting_style="none"),
                )
            writer.write_table(table)
            written += table.num_rows
        if writer is not None:
            writer.close()
    os.replace(tmp, args.dst)
    size_mb = os.path.getsize(args.dst) / 1e6
    print(f"wrote {written:,} rows -> {args.dst} ({size_mb:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
