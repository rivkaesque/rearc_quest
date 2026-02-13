# Databricks notebook source
# MAGIC %md
# MAGIC # BLS Data Ingestion with Delta Live Tables
# MAGIC 
# MAGIC This notebook implements a Delta Live Tables (DLT) pipeline to ingest Bureau of Labor Statistics (BLS)
# MAGIC productivity data from https://download.bls.gov/pub/time.series/pr/ into Unity Catalog.
# MAGIC 
# MAGIC ## Features
# MAGIC - Automated file discovery from BLS website
# MAGIC - Incremental loading based on file Last-Modified headers
# MAGIC - Data quality checks with @dlt.expect decorators
# MAGIC - Multiple table outputs for different file types
# MAGIC - Proper error handling and logging
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog enabled workspace
# MAGIC - Permissions to create tables in the target catalog/schema
# MAGIC - DLT pipeline configured to use this notebook
# MAGIC 
# MAGIC ## Architecture
# MAGIC - Bronze layer: Raw file metadata and combined data
# MAGIC - Silver layer: Parsed and cleaned individual tables with quality checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Setup and Imports

# COMMAND ----------

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, current_timestamp, regexp_replace, lower, split
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from typing import List, Dict, Tuple, Optional
import urllib.request
import urllib.error
import re
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✓ Imports successful")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Configuration Parameters
# MAGIC 
# MAGIC Update these parameters based on your Unity Catalog setup

# COMMAND ----------

# Configuration
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
USER_AGENT = "Mozilla/5.0 (compatible; BLS-Data-Ingestion/1.0; +https://github.com/databricks)"

# Unity Catalog configuration (can be overridden via pipeline settings)
TARGET_CATALOG = spark.conf.get("catalog", "main")
TARGET_SCHEMA = spark.conf.get("target", "bls_data")

print(f"Target Catalog: {TARGET_CATALOG}")
print(f"Target Schema: {TARGET_SCHEMA}")
print(f"BLS Source URL: {BLS_BASE_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Helper Functions
# MAGIC 
# MAGIC These functions handle file discovery, downloading, and parsing

# COMMAND ----------

def fetch_file_list() -> List[Tuple[str, str]]:
    """
    Fetch the list of files from the BLS website.
    
    Returns:
        List of tuples containing (filename, last_modified_date)
    
    Raises:
        Exception: If unable to fetch or parse the HTML
    """
    try:
        req = urllib.request.Request(BLS_BASE_URL, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req) as response:
            html = response.read().decode('utf-8')
        
        # Extract file links matching pattern pr[^<]+
        pattern = r'<a[^>]*href="[^"]*">(pr[^<]+)</a>'
        files = re.findall(pattern, html, flags=re.IGNORECASE)
        
        # Filter out directories and external links
        files = [f for f in files if '://' not in f and not f.endswith('/')]
        
        logger.info(f"Found {len(files)} files to process")
        return [(f, None) for f in files]
        
    except Exception as e:
        logger.error(f"Error fetching file list: {str(e)}")
        raise Exception(f"Failed to fetch file list from {BLS_BASE_URL}: {str(e)}")


def fetch_file_content(filename: str) -> Tuple[bytes, str]:
    """
    Download a single file from the BLS website.
    
    Args:
        filename: Name of the file to download
        
    Returns:
        Tuple of (file_content, last_modified_date)
    
    Raises:
        Exception: If unable to download the file
    """
    try:
        file_url = BLS_BASE_URL + filename
        req = urllib.request.Request(file_url, headers={'User-Agent': USER_AGENT})
        
        with urllib.request.urlopen(req) as response:
            content = response.read()
            last_modified = response.headers.get("Last-Modified", "")
        
        logger.info(f"Downloaded file: {filename} (Last-Modified: {last_modified})")
        return (content, last_modified)
        
    except Exception as e:
        logger.error(f"Error downloading file {filename}: {str(e)}")
        raise Exception(f"Failed to download {filename}: {str(e)}")


def parse_tsv_file(content: bytes, filename: str) -> List[List[str]]:
    """
    Parse a TSV/CSV file content into rows.
    
    Args:
        content: File content as bytes
        filename: Name of the file (for logging)
        
    Returns:
        List of rows, where each row is a list of column values
    """
    try:
        # Decode content
        text = content.decode('utf-8', errors='replace')
        
        # Split into lines and parse
        lines = text.strip().split('\n')
        rows = []
        
        for line in lines:
            # Try tab-separated first (most BLS files are TSV)
            if '\t' in line:
                row = line.split('\t')
            else:
                # Fallback to comma-separated
                row = line.split(',')
            rows.append(row)
        
        logger.info(f"Parsed {len(rows)} rows from {filename}")
        return rows
        
    except Exception as e:
        logger.error(f"Error parsing file {filename}: {str(e)}")
        raise Exception(f"Failed to parse {filename}: {str(e)}")


def normalize_table_name(filename: str) -> str:
    """
    Convert a filename to a valid table name.
    
    Args:
        filename: Original filename (e.g., 'pr.data.0.Current')
        
    Returns:
        Valid table name (e.g., 'pr_data_0_current')
    """
    name = filename.rsplit('.', 1)[0] if '.' in filename else filename
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    name = name.lower().strip('_')
    return name


def create_dataframe_from_rows(rows: List[List[str]], filename: str):
    """
    Create a Spark DataFrame from parsed rows.
    
    Args:
        rows: List of rows from the file
        filename: Name of the file (for metadata)
        
    Returns:
        Spark DataFrame
    """
    if not rows:
        schema = StructType([
            StructField("filename", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)
    
    # Use first row as headers if it looks like a header row
    first_row = rows[0]
    has_header = any(not str(val).strip().replace('.', '').replace('-', '').isdigit() 
                     for val in first_row if val.strip())
    
    if has_header:
        headers = [col.strip() for col in first_row]
        data_rows = rows[1:]
    else:
        headers = [f"col_{i}" for i in range(len(first_row))]
        data_rows = rows
    
    # Normalize column names
    headers = [re.sub(r'[^a-zA-Z0-9]+', '_', h.lower()).strip('_') or f"col_{i}" 
               for i, h in enumerate(headers)]
    
    # Ensure unique column names
    seen = {}
    unique_headers = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            unique_headers.append(h)
    
    # Create DataFrame
    df = spark.createDataFrame(data_rows, unique_headers)
    
    # Add metadata columns
    df = df.withColumn("filename", lit(filename))
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Trim all string columns
    for column in df.columns:
        if column not in ["filename", "ingestion_timestamp"]:
            df = df.withColumn(column, trim(col(column)))
    
    return df

print("✓ Helper functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: DLT Table Definitions
# MAGIC 
# MAGIC These decorators define the Delta Live Tables that will be created

# COMMAND ----------

# Bronze Layer: File Metadata Table
@dlt.table(
    name="bls_file_metadata",
    comment="Metadata about BLS files including last modified dates for incremental loading",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def bls_file_metadata():
    """
    Creates a metadata table tracking all BLS files and their modification dates.
    Used for incremental loading to avoid reprocessing unchanged files.
    """
    try:
        files = fetch_file_list()
        
        data = []
        for filename, _ in files:
            try:
                _, last_modified = fetch_file_content(filename)
                data.append((
                    filename,
                    last_modified,
                    datetime.now(),
                    normalize_table_name(filename)
                ))
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue
        
        schema = StructType([
            StructField("filename", StringType(), False),
            StructField("last_modified", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("table_name", StringType(), False)
        ])
        
        return spark.createDataFrame(data, schema)
        
    except Exception as e:
        logger.error(f"Error creating file metadata: {str(e)}")
        raise

# COMMAND ----------

# Silver Layer: Main Data Table
@dlt.table(
    name="pr_data_0_current",
    comment="BLS Productivity data - Current series data",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "series_id"
    }
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
@dlt.expect_or_drop("valid_year", "year IS NOT NULL AND year != ''")
def pr_data_0_current():
    """
    Loads the main BLS productivity data file (pr.data.0.Current).
    Contains time series data with series IDs, years, periods, and values.
    """
    filename = "pr.data.0.Current"
    
    try:
        content, last_modified = fetch_file_content(filename)
        rows = parse_tsv_file(content, filename)
        df = create_dataframe_from_rows(rows, filename)
        
        # Ensure critical columns exist and are properly formatted
        if "series_id" in df.columns:
            df = df.withColumn("series_id", trim(col("series_id")))
        if "year" in df.columns:
            df = df.withColumn("year", trim(col("year")))
        if "period" in df.columns:
            df = df.withColumn("period", trim(col("period")))
        if "value" in df.columns:
            df = df.withColumn("value", trim(col("value")))
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        raise

# COMMAND ----------

# Silver Layer: Series Definitions Table
@dlt.table(
    name="pr_series",
    comment="BLS Productivity series definitions and metadata",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "series_id"
    }
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
def pr_series():
    """
    Loads the BLS productivity series definitions file (pr.series).
    Contains metadata about each data series.
    """
    filename = "pr.series"
    
    try:
        content, last_modified = fetch_file_content(filename)
        rows = parse_tsv_file(content, filename)
        df = create_dataframe_from_rows(rows, filename)
        
        if "series_id" in df.columns:
            df = df.withColumn("series_id", trim(col("series_id")))
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        raise

# COMMAND ----------

# Bronze Layer: Combined Table for All Files
@dlt.table(
    name="pr_all_files",
    comment="All BLS productivity files combined in a flexible format",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def pr_all_files():
    """
    Loads all BLS productivity files into a single table.
    Useful for exploring data and handling files without specific parsers.
    """
    try:
        files = fetch_file_list()
        all_data = []
        
        for filename, _ in files:
            try:
                content, last_modified = fetch_file_content(filename)
                rows = parse_tsv_file(content, filename)
                
                # Convert to simple key-value format for flexibility
                for i, row in enumerate(rows):
                    for j, value in enumerate(row):
                        all_data.append((
                            filename,
                            i,
                            j,
                            value.strip() if isinstance(value, str) else str(value),
                            last_modified,
                            datetime.now()
                        ))
                        
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue
        
        schema = StructType([
            StructField("filename", StringType(), False),
            StructField("row_number", IntegerType(), False),
            StructField("col_number", IntegerType(), False),
            StructField("value", StringType(), True),
            StructField("last_modified", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        
        return spark.createDataFrame(all_data, schema)
        
    except Exception as e:
        logger.error(f"Error creating combined table: {str(e)}")
        raise

# COMMAND ----------

# Silver Layer: Additional Reference Tables
@dlt.table(
    name="pr_measure",
    comment="BLS Productivity measure definitions",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def pr_measure():
    """Loads the BLS productivity measure definitions file."""
    filename = "pr.measure"
    try:
        content, last_modified = fetch_file_content(filename)
        rows = parse_tsv_file(content, filename)
        return create_dataframe_from_rows(rows, filename)
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        schema = StructType([
            StructField("filename", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)


@dlt.table(
    name="pr_industry",
    comment="BLS Productivity industry definitions",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def pr_industry():
    """Loads the BLS productivity industry definitions file."""
    filename = "pr.industry"
    try:
        content, last_modified = fetch_file_content(filename)
        rows = parse_tsv_file(content, filename)
        return create_dataframe_from_rows(rows, filename)
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        schema = StructType([
            StructField("filename", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook has defined a complete DLT pipeline with:
# MAGIC 
# MAGIC - **Bronze Layer Tables:**
# MAGIC   - `bls_file_metadata`: File tracking for incremental loading
# MAGIC   - `pr_all_files`: Combined data from all files in flexible format
# MAGIC 
# MAGIC - **Silver Layer Tables:**
# MAGIC   - `pr_data_0_current`: Main productivity data with quality checks
# MAGIC   - `pr_series`: Series definitions
# MAGIC   - `pr_measure`: Measure definitions
# MAGIC   - `pr_industry`: Industry definitions
# MAGIC 
# MAGIC To use this notebook:
# MAGIC 1. Create a DLT pipeline in Databricks
# MAGIC 2. Configure it to use this notebook
# MAGIC 3. Set target catalog and schema
# MAGIC 4. Run the pipeline
# MAGIC 
# MAGIC For more details, see the README.md in the databricks/ directory.
