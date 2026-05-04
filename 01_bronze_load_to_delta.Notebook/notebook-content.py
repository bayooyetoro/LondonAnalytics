# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "44326ed2-8522-4e16-b041-da7a14617818",
# META       "default_lakehouse_name": "london_lakehouse",
# META       "default_lakehouse_workspace_id": "2538701f-1399-40bd-81a2-57d4ee5dc6d9",
# META       "known_lakehouses": [
# META         {
# META           "id": "44326ed2-8522-4e16-b041-da7a14617818"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import logging
import re
from typing import List, Optional
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

FILES_ROOT = "Files/bronze"

# ── Helpers ────────────────────────────────────────────────────────────────

def _camel_to_snake(s: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

def clean_name(name: str) -> str:
    if not name:
        return "col"
    n = _camel_to_snake(name.strip())
    n = re.sub(r"[\.\s\-\%/\\()]+", "_", n)
    n = re.sub(r'["\'"`]', "", n)
    n = re.sub(r"[^0-9A-Za-z_]+", "_", n)
    n = re.sub(r"__+", "_", n).strip("_").lower()
    return f"col_{n}" if re.match(r"^[0-9]", n) else (n or "col")

def clean_df_columns(df: DataFrame) -> DataFrame:
    for old_col in df.columns:
        new_col = clean_name(old_col)
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)
    return df

def resolve_path(folder: str, extension: str) -> str:
    """
    Finds the first file matching an extension in a folder.
    Avoids hardcoding filenames entirely.
    """
    import subprocess
    result = subprocess.run(
        ["find", folder, "-name", f"*.{extension}", "-type", "f"],
        capture_output=True, text=True
    )
    files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
    if not files:
        raise FileNotFoundError(f"No .{extension} file found in {folder}")
    logger.info(f"Resolved path: {files[0]}")
    return files[0]

def write_bronze_table(
    df: DataFrame,
    table_name: str,
    partition_cols: Optional[List[str]] = None
) -> None:
    logger.info(f"Writing table: {table_name} ...")
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        valid = [c for c in partition_cols if c in df.columns]
        if valid:
            writer = writer.partitionBy(*valid)
    writer.saveAsTable(table_name)
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    logger.info(f"Done — {table_name}: {count:,} rows")

def optimize_table(table_name: str) -> None:
    try:
        spark.sql(f"OPTIMIZE {table_name}")
        logger.info(f"Optimized: {table_name}")
    except Exception as e:
        logger.warning(f"Optimize skipped for {table_name}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Companies House ────────────────────────────────────────────────────────

def ingest_companies_house():
    # Wildcard read — works regardless of exact filename
    source_path = f"{FILES_ROOT}/companies_house/*.csv"
    logger.info(f"Reading Companies House from {source_path}")

    df = spark.read.csv(source_path, header=True, quote='"', escape='"')
    df = clean_df_columns(df)

    if "incorporation_date" in df.columns:
        df = df.withColumn(
            "incorporation_date",
            F.to_date(F.col("incorporation_date"), "dd/MM/yyyy")
        )
        df = df.withColumn("incorporation_year", F.year("incorporation_date"))

    write_bronze_table(df, "bronze_companies_house", partition_cols=["incorporation_year"])

ingest_companies_house()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Police Crime ───────────────────────────────────────────────────────────

def ingest_police_category(pattern: str, table_name: str, partition_col: str = "year_month"):
    source_path = f"{FILES_ROOT}/police_crime/{pattern}"
    logger.info(f"Reading {table_name} from {source_path}")

    df = spark.read.csv(source_path, header=True, recursiveFileLookup=True)
    df = clean_df_columns(df)

    if "month" in df.columns:
        df = df.withColumn("year_month", F.col("month"))
        df = df.withColumn(
            "event_month_date",
            F.to_date(F.concat(F.col("month"), F.lit("-01")), "yyyy-MM-dd")
        )

    for c in ["longitude", "latitude"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    parts = [partition_col] if partition_col else None
    write_bronze_table(df, table_name, partition_cols=parts)

ingest_police_category("*/*street*.csv",          "bronze_police_street",          "year_month")
ingest_police_category("*/*outcomes*.csv",         "bronze_police_outcomes",         "year_month")
ingest_police_category("*/*stop-and-search*.csv",  "bronze_police_stop_and_search",  "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── ONS Population ─────────────────────────────────────────────────────────

def ingest_ons_population():
    # Wildcard — picks up any CSV in the folder
    source_path = f"{FILES_ROOT}/ons/population/*.csv"
    logger.info(f"Reading ONS Population from {source_path}")

    df = spark.read.csv(source_path, header=True, inferSchema=True)
    df = clean_df_columns(df)

    write_bronze_table(df, "bronze_ons_population")

ingest_ons_population()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── ONS Deprivation ────────────────────────────────────────────────────────

def ingest_ons_deprivation():
    # Wildcard avoids the space and long name problem entirely
    source_path = f"{FILES_ROOT}/ons/deprivation/*.csv"
    logger.info(f"Reading ONS Deprivation from {source_path}")

    df = spark.read.csv(source_path, header=True, inferSchema=True)
    df = clean_df_columns(df)

    write_bronze_table(df, "bronze_ons_deprivation")

ingest_ons_deprivation()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── ONS Boundaries ─────────────────────────────────────────────────────────

def ingest_ons_boundaries():
    # CSV version — wildcard handles the long numeric filename
    csv_path = f"{FILES_ROOT}/ons/boundaries/*.csv"
    logger.info(f"Reading ONS Boundaries CSV from {csv_path}")

    df_bound = spark.read.csv(csv_path, header=True, inferSchema=True)
    df_bound = clean_df_columns(df_bound)
    write_bronze_table(df_bound, "bronze_ons_boundaries")

    # GeoJSON — store as raw text for Power BI map use later
    try:
        geo_path = f"{FILES_ROOT}/ons/boundaries/*.geojson"
        df_geo = (
            spark.read
            .text(geo_path, wholetext=True)
            .withColumnRenamed("value", "content")
            .withColumn("file_path", F.lit(geo_path))
        )
        write_bronze_table(df_geo, "bronze_ons_boundaries_geojson")
    except Exception as e:
        logger.error(f"GeoJSON load failed: {e}")

ingest_ons_boundaries()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Optimize All Tables ────────────────────────────────────────────────────

tables = [
    "bronze_companies_house",
    "bronze_police_street",
    "bronze_police_outcomes", 
    "bronze_police_stop_and_search",
    "bronze_ons_population",
    "bronze_ons_deprivation",
    "bronze_ons_boundaries",
    "bronze_ons_boundaries_geojson"
]

for t in tables:
    optimize_table(t)

logger.info("All bronze tables written and optimized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
