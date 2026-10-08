#!/usr/bin/env python3
"""L13 Part 1 (SOLUTION) - build a Great Expectations (GX Core 1.x) project for the raw taxi files.

    python solutions/part1_gx/gx_setup.py

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
        gxe.ExpectColumnValuesToBeOfType(column="fare_amount", type_="float64"),
        # --- volume: a normal month has ~2.5-3.5 M trips (Jan 2024: 2,964,624)
        gxe.ExpectTableRowCountToBeBetween(min_value=2_000_000, max_value=4_500_000),
        # --- completeness
        gxe.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime"),
        gxe.ExpectColumnValuesToNotBeNull(column="tpep_dropoff_datetime"),
        gxe.ExpectColumnValuesToNotBeNull(column="PULocationID"),
        # passenger_count is legitimately missing for ~5 % of trips (street-hail rows without the field)
        gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(column="passenger_count", min_value=0.90),
        # --- validity: ranges and accepted values. Real data is never 100 % clean -> `mostly`
        gxe.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0, max_value=1000, mostly=0.98),
        gxe.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0, max_value=100, mostly=0.99),
        gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=[0, 1, 2, 3, 4, 5, 6]),
        gxe.ExpectColumnValuesToBeBetween(column="PULocationID", min_value=1, max_value=265),
        # --- cross-column rule: a trip cannot end before it starts
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="tpep_dropoff_datetime",
                                                    column_B="tpep_pickup_datetime",
                                                    or_equal=True, mostly=0.999),
        # --- uniqueness: no natural key in TLC data -> a compound "business key" must be unique
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["VendorID", "tpep_pickup_datetime",
                                                         "tpep_dropoff_datetime", "PULocationID",
                                                         "DOLocationID", "total_amount"]),
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
    context.checkpoints.add_or_update(gx.Checkpoint(
        name="yellow_trips_raw_cp",
        validation_definitions=[vd],
        actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
        result_format={"result_format": "SUMMARY", "partial_unexpected_count": 5},
    ))

    print(f"GX project : {LAB / 'gx'}")
    print(f"data source: taxi_landing -> {LANDING}")
    print(f"suite      : {suite.name} ({len(suite.expectations)} expectations)")
    print("checkpoint : yellow_trips_raw_cp")
    print("next       : python part1_gx/run_checkpoint.py --month 2024-01")


if __name__ == "__main__":
    main()
