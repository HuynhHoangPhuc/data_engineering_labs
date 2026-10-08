#!/usr/bin/env python3
"""L13 Part 1 (STARTER) - build a Great Expectations (GX Core 1.x) project for the raw taxi files.

    python part1_gx/gx_setup.py          # complete the TODOs first; solution: solutions/part1_gx/gx_setup.py

Creates / updates, in a File Data Context at labs/L13_data_quality/gx/:

  Data Source        taxi_landing        pandas, filesystem, base_directory = data/landing/
  Data Asset         yellow_trips        Parquet files yellow_tripdata_YYYY-MM.parquet
  Batch Definition   monthly             one Batch = one file, chosen with batch_parameters {year, month}
  Expectation Suite  yellow_trips_raw    14 expectations (schema, volume, completeness, validity, uniqueness)
  Validation Def.    yellow_trips_raw_vd suite + batch definition
  Checkpoint         yellow_trips_raw_cp runs the validation definition, then rebuilds Data Docs

Re-running is safe: every object is created with add_or_update / get-or-create.
Run the checkpoint with:  python part1_gx/run_checkpoint.py --month 2024-01
"""
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context.types.base import ProgressBarsConfig

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())
LANDING = LAB / "data" / "landing"

TAXI_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
    "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "Airport_fee",
]


def build_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="yellow_trips_raw")
    expectations = [
        # --- schema: exact column set + types (pandas dtype names differ between pandas versions -> type list)
        gxe.ExpectTableColumnsToMatchSet(column_set=TAXI_COLUMNS, exact_match=True),
        gxe.ExpectColumnValuesToBeInTypeList(column="tpep_pickup_datetime",
                                             type_list=["datetime64[us]", "datetime64[ns]", "datetime64[ms]"]),
        # TODO 1 (schema): fare_amount must be of type float64           -> gxe.ExpectColumnValuesToBeOfType

        # --- volume
        # TODO 2 (volume): a normal month has ~2.5-3.5 M trips (Jan 2024: 2,964,624).
        #         Expect the row count to be between 2,000,000 and 4,500,000  -> gxe.ExpectTableRowCountToBeBetween

        # --- completeness
        gxe.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime"),
        # TODO 3 (completeness): tpep_dropoff_datetime and PULocationID must never be NULL.
        #         passenger_count is legitimately missing for ~5 % of trips: expect at least 90 % non-null
        #         -> gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(column=..., min_value=0.90)

        # --- validity. Real data is never 100 % clean -> use `mostly` (fraction of rows that must pass)
        gxe.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0, max_value=1000, mostly=0.98),
        # TODO 4 (validity):
        #   * trip_distance between 0 and 100 miles for at least 99 % of rows
        #   * payment_type only in the documented codes 0-6         -> gxe.ExpectColumnValuesToBeInSet
        #   * PULocationID between 1 and 265 (the taxi zone ids)

        # --- cross-column rule and uniqueness
        # TODO 5: a trip cannot end before it starts: tpep_dropoff_datetime >= tpep_pickup_datetime for 99.9 %
        #         -> gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A=..., column_B=..., or_equal=True, mostly=...)
        # TODO 6: TLC data has no trip id. Make the compound key (VendorID, tpep_pickup_datetime,
        #         tpep_dropoff_datetime, PULocationID, DOLocationID, total_amount) unique
        #         -> gxe.ExpectCompoundColumnsToBeUnique(column_list=[...])
    ]
    for e in expectations:
        suite.add_expectation(e)
    return suite


def main() -> None:
    # 1. Data Context: "file" mode keeps all configuration as YAML/JSON under LAB/gx (commit it in real projects)
    context = gx.get_context(mode="file", project_root_dir=str(LAB))
    context.variables.progress_bars = ProgressBarsConfig(globally=False)  # no tqdm bars in our console output
    context.variables.save()

    # 2. Data Source -> Data Asset -> Batch Definition
    ds = context.data_sources.add_or_update_pandas_filesystem(name="taxi_landing", base_directory=str(LANDING))
    asset = ds.get_asset("yellow_trips") if "yellow_trips" in ds.get_asset_names() \
        else ds.add_parquet_asset(name="yellow_trips")
    if "monthly" in [bd.name for bd in asset.batch_definitions]:
        batch_def = asset.get_batch_definition("monthly")
    else:
        batch_def = asset.add_batch_definition_monthly(
            name="monthly", regex=r"yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.parquet")

    # 3. Expectation Suite
    suite = context.suites.add_or_update(build_suite())

    # 4. Validation Definition = WHAT data (batch definition) + WHICH rules (suite)
    vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="yellow_trips_raw_vd", data=batch_def, suite=suite))

    # 5. Checkpoint = validation definition(s) + actions (Data Docs; Slack/email/... in production)
    # TODO 7: create (add_or_update) a gx.Checkpoint named "yellow_trips_raw_cp" that
    #   * runs [vd]
    #   * has one action: gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")
    #   * uses result_format={"result_format": "SUMMARY", "partial_unexpected_count": 5}
    # context.checkpoints.add_or_update(gx.Checkpoint(...))

    print(f"GX project : {LAB / 'gx'}")
    print(f"data source: taxi_landing -> {LANDING}")
    print(f"suite      : {suite.name} ({len(suite.expectations)} expectations)")
    print("checkpoint : yellow_trips_raw_cp")
    print("next       : python part1_gx/run_checkpoint.py --month 2024-01")


if __name__ == "__main__":
    main()
