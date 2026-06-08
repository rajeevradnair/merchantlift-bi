"""Build slowly changing dimension tables from Silver dimensions."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import SILVER_DIR
from merchantlift.scd_config import SCD_DIMENSIONS, ScdDimensionConfig
from merchantlift.spark import create_spark_session, create_spark_session_local


SCD_RULE_VERSION = "scd_rules_v1"
INITIAL_EFFECTIVE_START_DATE = "2026-01-01"


def build_pipeline_run_id() -> str:
    """Create a unique SCD pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"scd_run_{timestamp}"


def get_silver_table_path(table_name: str):
    """Return Silver Delta table path.

    Args:
        table_name: Silver table name.

    Returns:
        Silver table path.
    """
    return SILVER_DIR / table_name


def read_silver_table(
    spark,
    table_name: str,
) -> DataFrame:
    """Read one Silver Delta table.

    Args:
        spark: Active Spark session.
        table_name: Silver table name.

    Returns:
        Silver DataFrame.
    """
    return (
        spark.read
        .format("delta")
        .load(str(get_silver_table_path(table_name)))
    )


def require_columns(
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """Validate that required columns exist.

    Args:
        df: DataFrame to validate.
        table_name: Human-readable table name.
        required_columns: Required column names.

    Raises:
        ValueError: If any required columns are missing.
    """
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def inspect_dimension_source(
    table_name: str,
    df: DataFrame,
    sample_size: int = 5,
) -> int:
    """Inspect one Silver dimension source table.

    Args:
        table_name: Silver source table name.
        df: Source DataFrame.
        sample_size: Number of sample rows to show.

    Returns:
        Row count.
    """
    print("\n" + "=" * 80)
    print(f"Inspecting Silver dimension source: {table_name}")
    print("=" * 80)

    print("\nSchema:")
    df.printSchema()

    row_count = df.count()

    print("\nRow count:")
    print(f"{table_name}: {row_count:,} rows")

    print("\nSample rows:")
    df.show(sample_size, truncate=False)

    return row_count


def read_and_validate_dimension_source(
    spark,
    config: ScdDimensionConfig,
) -> tuple[DataFrame, int]:
    """Read and validate one SCD source dimension.

    Args:
        spark: Active Spark session.
        config: SCD dimension configuration.

    Returns:
        Source DataFrame and row count.
    """
    source_df = read_silver_table(
        spark=spark,
        table_name=config.source_table_name,
    )

    required_columns = (
        (config.business_key,)
        + config.tracked_columns
    )

    require_columns(
        df=source_df,
        table_name=config.source_table_name,
        required_columns=required_columns,
    )

    row_count = inspect_dimension_source(
        table_name=config.source_table_name,
        df=source_df,
    )

    return source_df, row_count


def build_tracked_column_hash(
    df: DataFrame,
    tracked_columns: tuple[str, ...],
) -> DataFrame:
    """Add a hash over tracked SCD columns.

    Args:
        df: Source dimension DataFrame.
        tracked_columns: Columns whose changes should create new SCD versions.

    Returns:
        DataFrame with scd_record_hash.
    """
    hash_inputs = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("__NULL__"))
        for column_name in tracked_columns
    ]

    return df.withColumn(
        "scd_record_hash",
        F.sha2(
            F.concat_ws("||", *hash_inputs),
            256,
        ),
    )


def write_silver_table(
    df: DataFrame,
    table_name: str,
) -> None:
    """Write one Silver Delta table.

    Args:
        df: DataFrame to write.
        table_name: Output Silver table name.
    """
    output_path = get_silver_table_path(table_name)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )

    print(f"Wrote SCD table: {output_path}")


def validate_written_scd_table(
    spark,
    table_name: str,
    expected_row_count: int,
) -> None:
    """Validate a written SCD Delta table.

    Args:
        spark: Active Spark session.
        table_name: Written SCD table name.
        expected_row_count: Expected row count.

    Raises:
        ValueError: If the written row count does not match.
    """
    written_df = read_silver_table(
        spark=spark,
        table_name=table_name,
    )

    actual_row_count = written_df.count()

    print("\nWritten SCD table validation")
    print("=" * 80)
    print(f"{'table':<40} {table_name}")
    print(f"{'expected rows':<40} {expected_row_count:>12,}")
    print(f"{'actual rows':<40} {actual_row_count:>12,}")
    print("=" * 80)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"SCD row count mismatch for {table_name}: "
            f"expected {expected_row_count:,}, got {actual_row_count:,}"
        )

    print(f"SCD write validation passed: {table_name}")


def validate_scd_output_columns(
    scd_df: DataFrame,
    config: ScdDimensionConfig,
) -> None:
    """Validate required SCD output columns.

    Args:
        scd_df: SCD DataFrame.
        config: SCD dimension configuration.
    """
    required_columns = (
        "surrogate_scd_id",
        config.business_key,
        *config.tracked_columns,
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "scd_record_hash",
        "scd_created_at",
        "scd_updated_at",
        "scd_pipeline_run_id",
        "scd_rule_version",
    )

    require_columns(
        df=scd_df,
        table_name=config.output_table_name,
        required_columns=required_columns,
    )


def write_and_validate_scd_dimension(
    spark,
    config: ScdDimensionConfig,
    scd_df: DataFrame,
    expected_row_count: int,
) -> None:
    """Write and validate one SCD dimension table.

    Args:
        spark: Active Spark session.
        config: SCD dimension configuration.
        scd_df: SCD DataFrame to write.
        expected_row_count: Expected row count.
    """
    validate_scd_output_columns(
        scd_df=scd_df,
        config=config,
    )

    write_silver_table(
        df=scd_df,
        table_name=config.output_table_name,
    )

    validate_written_scd_table(
        spark=spark,
        table_name=config.output_table_name,
        expected_row_count=expected_row_count,
    )


def validate_scd_business_rules(
    spark,
    config: ScdDimensionConfig,
) -> tuple[str, int]:
    """Validate business rules for one written SCD table.

    Args:
        spark: Active Spark session.
        config: SCD dimension configuration.

    Returns:
        Output table name and validation failure count.
    """
    scd_df = read_silver_table(
        spark=spark,
        table_name=config.output_table_name,
    )

    require_columns(
        df=scd_df,
        table_name=config.output_table_name,
        required_columns=(
            "surrogate_scd_id",
            config.business_key,
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "scd_record_hash",
            "scd_created_at",
            "scd_updated_at",
            "scd_pipeline_run_id",
            "scd_rule_version",
        ),
    )

    null_required_count = (
        scd_df
        .filter(
            F.col("surrogate_scd_id").isNull()
            | F.col(config.business_key).isNull()
            | F.col("effective_start_date").isNull()
            | F.col("is_current").isNull()
            | F.col("scd_record_hash").isNull()
            | F.col("scd_created_at").isNull()
            | F.col("scd_updated_at").isNull()
            | F.col("scd_pipeline_run_id").isNull()
            | F.col("scd_rule_version").isNull()
        )
        .count()
    )

    invalid_effective_dates_count = (
        scd_df
        .filter(
            F.col("effective_end_date").isNotNull()
            & (F.col("effective_end_date") < F.col("effective_start_date"))
        )
        .count()
    )

    invalid_current_end_date_count = (
        scd_df
        .filter(
            (F.col("is_current") == True)
            & F.col("effective_end_date").isNotNull()
        )
        .count()
    )

    current_rows_per_key_df = (
        scd_df
        .filter(F.col("is_current") == True)
        .groupBy(config.business_key)
        .count()
    )

    invalid_current_key_count = (
        current_rows_per_key_df
        .filter(F.col("count") != 1)
        .count()
    )

    duplicate_surrogate_id_count = (
        scd_df
        .groupBy("surrogate_scd_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    failure_count = (
        null_required_count
        + invalid_effective_dates_count
        + invalid_current_end_date_count
        + invalid_current_key_count
        + duplicate_surrogate_id_count
    )

    print("\nSCD business-rule validation")
    print("=" * 80)
    print(f"{'table':<45} {config.output_table_name}")
    print(f"{'null required rows':<45} {null_required_count:>12,}")
    print(f"{'invalid effective date rows':<45} {invalid_effective_dates_count:>12,}")
    print(f"{'current rows with end date':<45} {invalid_current_end_date_count:>12,}")
    print(f"{'business keys without exactly one current row':<45} {invalid_current_key_count:>12,}")
    print(f"{'duplicate surrogate_scd_id values':<45} {duplicate_surrogate_id_count:>12,}")
    print(f"{'total validation failures':<45} {failure_count:>12,}")
    print("=" * 80)

    return config.output_table_name, failure_count


def validate_all_scd_outputs(spark) -> None:
    """Validate all configured SCD output tables.

    Args:
        spark: Active Spark session.

    Raises:
        ValueError: If any SCD table fails validation.
    """
    print("\nValidating all SCD outputs")
    print("=" * 80)

    validation_results: list[tuple[str, int]] = []

    for config in SCD_DIMENSIONS:
        table_name, failure_count = validate_scd_business_rules(
            spark=spark,
            config=config,
        )

        validation_results.append(
            (
                table_name,
                failure_count,
            )
        )

    print("\nSCD validation summary")
    print("=" * 80)
    print(f"{'table':<45} {'failures':>12} {'status':>12}")
    print("-" * 80)

    failed_tables = []

    for table_name, failure_count in validation_results:
        status = "PASSED" if failure_count == 0 else "FAILED"

        if failure_count > 0:
            failed_tables.append(table_name)

        print(f"{table_name:<45} {failure_count:>12,} {status:>12}")

    print("=" * 80)

    if failed_tables:
        raise ValueError(
            "SCD output validation failed for: "
            + ", ".join(failed_tables)
        )

    print("All SCD output validations passed.")



def add_surrogate_scd_id(
    df: DataFrame,
    business_key: str,
) -> DataFrame:
    """Add deterministic surrogate SCD ID.

    Args:
        df: SCD DataFrame.
        business_key: Natural/business key column.

    Returns:
        DataFrame with surrogate_scd_id.
    """
    return df.withColumn(
        "surrogate_scd_id",
        F.concat(
            F.lit("scd_"),
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col(business_key).cast("string"),
                    F.col("effective_start_date").cast("string"),
                    F.col("scd_record_hash"),
                ),
                256,
            ),
        ),
    )


def build_initial_scd_snapshot(
    source_df: DataFrame,
    config: ScdDimensionConfig,
    pipeline_run_id: str,
) -> DataFrame:
    """Build an initial SCD Type 2 snapshot from a clean dimension.

    Args:
        source_df: Clean Silver dimension DataFrame.
        config: SCD dimension configuration.
        pipeline_run_id: SCD pipeline run identifier.

    Returns:
        Initial SCD snapshot DataFrame.
    """
    require_columns(
        df=source_df,
        table_name=config.source_table_name,
        required_columns=(config.business_key,) + config.tracked_columns,
    )

    deduplicated_source_df = (
        source_df
        .filter(F.col(config.business_key).isNotNull())
        .dropDuplicates([config.business_key])
    )

    hashed_df = build_tracked_column_hash(
        df=deduplicated_source_df,
        tracked_columns=config.tracked_columns,
    )

    scd_df = (
        hashed_df
        .withColumn(
            "effective_start_date",
            F.to_date(F.lit(INITIAL_EFFECTIVE_START_DATE)),
        )
        .withColumn(
            "effective_end_date",
            F.lit(None).cast("date"),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
        .withColumn(
            "scd_created_at",
            F.current_timestamp(),
        )
        .withColumn(
            "scd_updated_at",
            F.current_timestamp(),
        )
        .withColumn(
            "scd_pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "scd_rule_version",
            F.lit(SCD_RULE_VERSION),
        )
    )

    scd_with_id_df = add_surrogate_scd_id(
        df=scd_df,
        business_key=config.business_key,
    )

    metadata_columns = [
        "surrogate_scd_id",
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "scd_record_hash",
        "scd_created_at",
        "scd_updated_at",
        "scd_pipeline_run_id",
        "scd_rule_version",
    ]

    return scd_with_id_df.select(
        "surrogate_scd_id",
        config.business_key,
        *config.tracked_columns,
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "scd_record_hash",
        "scd_created_at",
        "scd_updated_at",
        "scd_pipeline_run_id",
        "scd_rule_version",
    )


def build_one_scd_dimension(
    spark,
    config: ScdDimensionConfig,
    pipeline_run_id: str,
) -> tuple[str, int, int, DataFrame]:
    """Build one initial SCD dimension from one Silver source table.

    Args:
        spark: Active Spark session.
        config: SCD dimension configuration.
        pipeline_run_id: SCD pipeline run identifier.

    Returns:
        Output table name, source row count, SCD row count, and SCD DataFrame.
    """
    print("\n" + "=" * 80)
    print(f"Building SCD dimension: {config.output_table_name}")
    print(f"Source table: {config.source_table_name}")
    print(f"Business key: {config.business_key}")
    print(f"Tracked columns: {', '.join(config.tracked_columns)}")
    print("=" * 80)

    source_df = read_silver_table(
        spark=spark,
        table_name=config.source_table_name,
    )

    source_count = source_df.count()

    scd_df = build_initial_scd_snapshot(
        source_df=source_df,
        config=config,
        pipeline_run_id=pipeline_run_id,
    )

    scd_count = scd_df.count()

    print("\nSCD build result")
    print("=" * 80)
    print(f"{'source rows':<40} {source_count:>12,}")
    print(f"{'scd rows':<40} {scd_count:>12,}")
    print(f"{'rows removed':<40} {source_count - scd_count:>12,}")
    print("=" * 80)

    print("\nSCD schema")
    scd_df.printSchema()

    print("\nSCD sample")
    scd_df.show(10, truncate=False)

    return config.output_table_name, source_count, scd_count, scd_df



def main(spark_session=None) -> None:
    """Run SCD dimension build."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-read-bronze-table")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    pipeline_run_id = build_pipeline_run_id()

    print("Starting SCD dimension build")
    print("=" * 80)
    print(f"Silver directory: {SILVER_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"SCD rule version: {SCD_RULE_VERSION}")
    print(f"Configured dimensions: {len(SCD_DIMENSIONS)}")
    print("=" * 80)

    source_counts: list[tuple[str, int]] = []

    for config in SCD_DIMENSIONS:
        print(
            f"\nConfigured SCD: {config.source_table_name} "
            f"-> {config.output_table_name}"
        )

        _, row_count = read_and_validate_dimension_source(
            spark=spark,
            config=config,
        )

        source_counts.append(
            (
                config.source_table_name,
                row_count,
            )
        )

    print("\nSCD source dimension summary")
    print("=" * 80)
    print(f"{'source_table':<40} {'rows':>12}")
    print("-" * 80)

    total_rows = 0

    for table_name, row_count in source_counts:
        total_rows += row_count
        print(f"{table_name:<40} {row_count:>12,}")

    print("-" * 80)
    print(f"{'TOTAL':<40} {total_rows:>12,}")
    print("=" * 80)

    scd_results: list[tuple[str, int, int]] = []
    scd_dataframes: dict[str, DataFrame] = {}

    scd_results: list[tuple[ScdDimensionConfig, int, int, DataFrame]] = []

    for config in SCD_DIMENSIONS:
        output_table_name, source_count, scd_count, scd_df = build_one_scd_dimension(
            spark=spark,
            config=config,
            pipeline_run_id=pipeline_run_id,
        )

        scd_results.append(
            (
                config,
                source_count,
                scd_count,
                scd_df,
            )
        )

    print("\nSCD dimension build summary")
    print("=" * 80)
    print(
        f"{'output_table':<35} "
        f"{'source_rows':>12} "
        f"{'scd_rows':>12} "
        f"{'removed':>12}"
    )
    print("-" * 80)

    total_source_rows = 0
    total_scd_rows = 0

    for config, source_count, scd_count, _ in scd_results:
        total_source_rows += source_count
        total_scd_rows += scd_count

        print(
            f"{config.output_table_name:<35} "
            f"{source_count:>12,} "
            f"{scd_count:>12,} "
            f"{source_count - scd_count:>12,}"
        )

    print("-" * 80)
    print(
        f"{'TOTAL':<35} "
        f"{total_source_rows:>12,} "
        f"{total_scd_rows:>12,} "
        f"{total_source_rows - total_scd_rows:>12,}"
    )
    print("=" * 80)

    print("\nWriting SCD Delta tables")
    print("=" * 80)

    for config, _, scd_count, scd_df in scd_results:
        write_and_validate_scd_dimension(
            spark=spark,
            config=config,
            scd_df=scd_df,
            expected_row_count=scd_count,
        )

    print("\nAll SCD Delta tables written and validated.")

    validate_all_scd_outputs(spark)

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()