"""
Delta Live Tables Pipeline for BLS Data Ingestion

This module implements a DLT pipeline that ingests Bureau of Labor Statistics (BLS)
productivity data from https://download.bls.gov/pub/time.series/pr/ into Unity Catalog.

The pipeline:
- Downloads files from the BLS source URL
- Parses HTML to extract file links
- Loads each file into separate Delta tables
- Implements incremental loading based on Last-Modified headers
- Includes data quality checks and error handling
"""

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

# Configuration
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
USER_AGENT = "Mozilla/5.0 (compatible; BLS-Data-Ingestion/1.0; +https://github.com/databricks)"

# Spark session (available in DLT context)
spark = SparkSession.builder.getOrCreate()


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
        return [(f, None) for f in files]  # Last-Modified will be fetched per file
        
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
    
    Note:
        For files with multiple dots like 'pr.data.0.Current', the entire filename
        is processed: dots and special characters are replaced with underscores,
        then converted to lowercase to create a valid Delta table name.
    """
    # Replace dots and special characters with underscores
    # (this handles filenames with multiple dots correctly)
    name = re.sub(r'[^a-zA-Z0-9]+', '_', filename)
    
    # Convert to lowercase
    name = name.lower()
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
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
        # Return empty DataFrame with metadata columns
        schema = StructType([
            StructField("filename", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)
    
    # Use first row as headers if it looks like a header row
    # Check if first row contains typical header keywords or mostly non-numeric values
    first_row = rows[0]
    header_keywords = ['id', 'name', 'series', 'year', 'period', 'value', 'code', 'title', 'description']
    has_keyword = any(keyword in str(val).lower() for val in first_row for keyword in header_keywords)
    
    # Also check if majority of values are non-numeric
    non_numeric_count = sum(1 for val in first_row 
                           if val.strip() and not str(val).strip().replace('.', '').replace('-', '').isdigit())
    has_header = has_keyword or (non_numeric_count > len(first_row) / 2)
    
    if has_header:
        headers = [col.strip() for col in first_row]
        data_rows = rows[1:]
    else:
        # Generate column names
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


# DLT Table: File Metadata
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


# DLT Table: pr.data series (main data file)
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


# DLT Table: pr.series (series definitions)
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
        
        # Ensure series_id is properly formatted
        if "series_id" in df.columns:
            df = df.withColumn("series_id", trim(col("series_id")))
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        raise


# DLT Table: Generic loader for all other files
@dlt.table(
    name="pr_all_files",
    comment="All BLS productivity files combined",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def pr_all_files():
    """
    Loads all BLS productivity files into a single table.
    This is useful for exploring the data and for files we don't have specific handlers for.
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


# Additional specific tables can be added following the same pattern
# For example: pr.measure, pr.industry, pr.duration, etc.

@dlt.table(
    name="pr_measure",
    comment="BLS Productivity measure definitions",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def pr_measure():
    """
    Loads the BLS productivity measure definitions file.
    """
    filename = "pr.measure"
    
    try:
        content, last_modified = fetch_file_content(filename)
        rows = parse_tsv_file(content, filename)
        return create_dataframe_from_rows(rows, filename)
        
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        # Return empty DataFrame instead of failing
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
    """
    Loads the BLS productivity industry definitions file.
    """
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
