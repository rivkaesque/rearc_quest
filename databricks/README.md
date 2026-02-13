# Databricks Delta Live Tables - BLS Data Ingestion

This directory contains a Databricks Delta Live Tables (DLT) implementation for ingesting Bureau of Labor Statistics (BLS) productivity data into Unity Catalog.

## Overview

The DLT pipeline provides an alternative to the AWS Lambda/S3 implementation, offering:
- Automated data ingestion from BLS website
- Delta tables stored in Unity Catalog
- Built-in data quality checks
- Incremental loading capabilities
- Integrated lineage tracking
- Native Databricks scheduling and monitoring

## Files

- **`dlt_bls_pipeline.py`**: Main DLT pipeline implementation (Python module format)
- **`BLS_Data_Ingestion.py`**: Same pipeline as Databricks notebook format
- **`dlt_config.json`**: Pipeline configuration template
- **`README.md`**: This documentation file

## Prerequisites

### Unity Catalog Setup
1. **Unity Catalog Enabled**: Ensure your Databricks workspace has Unity Catalog enabled
2. **Catalog Access**: You need `CREATE SCHEMA` and `CREATE TABLE` permissions on the target catalog
3. **Compute Access**: Access to create and run DLT pipelines

### Required Permissions
```sql
-- Grant permissions (run as catalog admin)
GRANT CREATE SCHEMA ON CATALOG <catalog_name> TO `<user_or_group>`;
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<user_or_group>`;
```

### Network Access
- Outbound HTTPS access to `download.bls.gov` (port 443)
- No special firewall rules typically needed for Databricks compute

## Deployment

### Option 1: Deploy as Notebook (Recommended for Development)

1. **Import Notebook**:
   - Go to Databricks Workspace
   - Navigate to your desired folder
   - Click "Import"
   - Upload `BLS_Data_Ingestion.py`

2. **Create DLT Pipeline**:
   - Go to "Workflows" → "Delta Live Tables"
   - Click "Create Pipeline"
   - Configure:
     - **Pipeline Name**: `bls_data_ingestion`
     - **Notebook Libraries**: Select the imported notebook
     - **Target**: `bls_data` (schema name)
     - **Catalog**: `main` (or your catalog name)
     - **Cluster Mode**: Fixed size or Autoscaling
     - **Storage Location**: Leave default or specify Unity Catalog managed location

3. **Configure Pipeline Settings** (Optional):
   ```json
   {
     "bls.base_url": "https://download.bls.gov/pub/time.series/pr/",
     "bls.user_agent": "Mozilla/5.0 (compatible; BLS-Data-Ingestion/1.0)"
   }
   ```

4. **Start Pipeline**:
   - Click "Start" to run the pipeline
   - Monitor progress in the pipeline UI

### Option 2: Deploy Using Databricks CLI

1. **Install Databricks CLI**:
   ```bash
   pip install databricks-cli
   ```

2. **Configure Authentication**:
   ```bash
   databricks configure --token
   ```

3. **Upload Pipeline Files**:
   ```bash
   # Upload notebook
   databricks workspace import BLS_Data_Ingestion.py \
     /Workspace/Users/<your-email>/BLS_Data_Ingestion.py \
     --language PYTHON
   ```

4. **Create Pipeline Using API**:
   ```bash
   # Update dlt_config.json with your settings first
   databricks pipelines create --settings dlt_config.json
   ```

### Option 3: Deploy Using Terraform

```hcl
resource "databricks_notebook" "bls_ingestion" {
  source = "${path.module}/BLS_Data_Ingestion.py"
  path   = "/Workspace/Users/${var.user_email}/BLS_Data_Ingestion"
}

resource "databricks_pipeline" "bls_pipeline" {
  name    = "bls_data_ingestion"
  storage = "/tmp/dlt/bls_data_ingestion"
  
  catalog = "main"
  target  = "bls_data"

  library {
    notebook {
      path = databricks_notebook.bls_ingestion.path
    }
  }

  cluster {
    label = "default"
    autoscale {
      min_workers = 1
      max_workers = 5
    }
  }

  continuous = false
  development = true
}
```

## Pipeline Configuration

### Catalog and Schema
Update these in your pipeline configuration:
- **Catalog**: Unity Catalog name (default: `main`)
- **Target Schema**: Schema for tables (default: `bls_data`)

Tables will be created as: `<catalog>.<schema>.<table_name>`

Example: `main.bls_data.pr_data_0_current`

### Cluster Configuration
The pipeline uses a single-node or multi-node cluster:
- **Development**: 1-2 workers (faster startup, lower cost)
- **Production**: 1-5 workers with autoscaling
- **Recommended Instance**: i3.xlarge or similar compute-optimized instance

### Storage
- Unity Catalog managed storage (recommended)
- Or custom location in your cloud storage

## Tables Created

The pipeline creates the following tables in your Unity Catalog:

### Bronze Layer
| Table Name | Description | Schema |
|------------|-------------|--------|
| `bls_file_metadata` | File tracking for incremental loading | filename, last_modified, ingestion_timestamp, table_name |
| `pr_all_files` | All files in key-value format | filename, row_number, col_number, value, last_modified, ingestion_timestamp |

### Silver Layer
| Table Name | Description | Quality Checks |
|------------|-------------|----------------|
| `pr_data_0_current` | Main productivity data | series_id NOT NULL, year NOT NULL |
| `pr_series` | Series definitions | series_id NOT NULL |
| `pr_measure` | Measure definitions | None |
| `pr_industry` | Industry definitions | None |

## Scheduling

### Option 1: Using DLT Pipeline Scheduler (Recommended)

1. Open your DLT pipeline
2. Click "Schedule"
3. Configure:
   - **Schedule Type**: Triggered or Scheduled
   - **Cron Expression**: `0 2 * * *` (daily at 2 AM)
   - **Timezone**: Your timezone

### Option 2: Using Databricks Jobs

```python
# Create a job that triggers the DLT pipeline
from databricks_cli.sdk import JobsService

job_config = {
    "name": "BLS Data Ingestion - Daily",
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "UTC"
    },
    "tasks": [{
        "task_key": "run_dlt_pipeline",
        "pipeline_task": {
            "pipeline_id": "<your-pipeline-id>"
        }
    }]
}
```

### Option 3: Using Terraform

```hcl
resource "databricks_job" "bls_ingestion_job" {
  name = "BLS Data Ingestion - Daily"

  schedule {
    quartz_cron_expression = "0 0 2 * * ?"
    timezone_id            = "UTC"
  }

  task {
    task_key = "run_dlt_pipeline"
    
    pipeline_task {
      pipeline_id = databricks_pipeline.bls_pipeline.id
    }
  }
}
```

## Incremental Loading

The pipeline implements incremental loading by:
1. Checking file `Last-Modified` headers from the BLS website
2. Comparing with previously ingested timestamps in `bls_file_metadata`
3. Only reprocessing files that have been updated

To force a full refresh:
- Delete the `bls_file_metadata` table
- Run the pipeline with "Full Refresh" mode

## Data Quality Monitoring

### Built-in Quality Checks
The pipeline includes data quality expectations:
- **pr_data_0_current**: Drops rows with missing series_id or year
- **pr_series**: Drops rows with missing series_id

### Monitoring Quality Metrics
1. Open your DLT pipeline
2. View "Data Quality" tab
3. Monitor:
   - Records processed
   - Records dropped
   - Expectation failures

### Custom Quality Checks
Add custom checks by modifying the pipeline:
```python
@dlt.expect("valid_value", "value IS NOT NULL")
@dlt.expect_or_fail("critical_check", "series_id LIKE 'PRS%'")
```

## Creating Dashboards

### Step 1: Query Tables

```sql
-- Example: Productivity trends
SELECT 
    s.series_title,
    d.year,
    d.period,
    CAST(d.value AS DOUBLE) as value
FROM main.bls_data.pr_data_0_current d
INNER JOIN main.bls_data.pr_series s 
    ON TRIM(d.series_id) = TRIM(s.series_id)
WHERE d.year >= '2020'
    AND d.period NOT IN ('M13', 'Q05')
ORDER BY d.year, d.period
```

### Step 2: Create Visualizations

1. **Create SQL Query**:
   - Go to "SQL Editor" in Databricks
   - Write your query
   - Save the query

2. **Add Visualizations**:
   - Click "Add Visualization"
   - Choose chart type (Line, Bar, etc.)
   - Configure axes and groupings

3. **Create Dashboard**:
   - Go to "Dashboards"
   - Click "Create Dashboard"
   - Add your saved queries/visualizations
   - Arrange and format

### Step 3: Share Dashboard

1. Click "Share" on your dashboard
2. Options:
   - **User/Group Access**: Grant view/edit permissions
   - **Public Link**: Generate shareable link
   - **Schedule Email**: Email dashboard snapshots
   - **Embed**: Embed in external applications

### Example Dashboard Queries

**Query 1: Top Series by Latest Value**
```sql
SELECT 
    s.series_title,
    d.series_id,
    MAX(CAST(d.value AS DOUBLE)) as max_value
FROM main.bls_data.pr_data_0_current d
INNER JOIN main.bls_data.pr_series s ON d.series_id = s.series_id
WHERE d.year = (SELECT MAX(year) FROM main.bls_data.pr_data_0_current)
GROUP BY s.series_title, d.series_id
ORDER BY max_value DESC
LIMIT 10
```

**Query 2: Year-over-Year Change**
```sql
WITH yearly_data AS (
    SELECT 
        series_id,
        year,
        AVG(CAST(value AS DOUBLE)) as avg_value
    FROM main.bls_data.pr_data_0_current
    WHERE period LIKE 'Q%'
    GROUP BY series_id, year
)
SELECT 
    curr.series_id,
    curr.year,
    curr.avg_value as current_value,
    prev.avg_value as previous_value,
    ((curr.avg_value - prev.avg_value) / prev.avg_value * 100) as yoy_change_pct
FROM yearly_data curr
LEFT JOIN yearly_data prev 
    ON curr.series_id = prev.series_id 
    AND CAST(curr.year AS INT) = CAST(prev.year AS INT) + 1
WHERE curr.year >= '2020'
ORDER BY curr.series_id, curr.year
```

## Comparison with AWS Lambda/S3 Approach

| Feature | AWS Lambda/S3 | Databricks DLT |
|---------|---------------|----------------|
| **Deployment** | SAM/CloudFormation | DLT Pipeline Creation |
| **Storage** | S3 (Raw files + index.html) | Unity Catalog (Delta tables) |
| **Data Format** | Raw files (TSV/CSV) | Structured Delta tables |
| **Scheduling** | CloudWatch Events | DLT Scheduler or Jobs |
| **Monitoring** | CloudWatch Logs | DLT Pipeline UI + Lineage |
| **Query Access** | Athena or download files | SQL, Python, or dashboards |
| **Data Quality** | Manual validation | Built-in expectations |
| **Incremental Load** | Manual Last-Modified check | Built-in change tracking |
| **Versioning** | S3 versioning | Delta time travel |
| **Cost Model** | Lambda invocations + S3 storage | Cluster compute + storage |
| **Sharing** | S3 bucket policies + URL | Unity Catalog permissions + dashboards |
| **Best For** | Simple file sync | Analytics and reporting |

### When to Use Each Approach

**Use AWS Lambda/S3 when:**
- You need simple file synchronization
- External systems require direct file access
- You want to minimize compute costs for infrequent access
- You're already using AWS infrastructure

**Use Databricks DLT when:**
- You need structured analytics-ready data
- You want built-in data quality and lineage
- You need to join with other Databricks datasets
- You want interactive dashboards and SQL access
- You need advanced transformations and aggregations

## Troubleshooting

### Common Issues

**Issue: "Catalog not found"**
```
Solution: Verify Unity Catalog name and permissions:
- Check catalog exists: SHOW CATALOGS
- Check permissions: SHOW GRANTS ON CATALOG <name>
```

**Issue: "Failed to fetch file list"**
```
Solution: Network connectivity issue
- Verify outbound HTTPS access to download.bls.gov
- Check User-Agent header is set correctly
- Verify BLS website is accessible
```

**Issue: "Schema inference failed"**
```
Solution: File format issue
- Check file is tab-separated (most BLS files are TSV)
- Verify file encoding (should be UTF-8)
- Add error handling for specific files
```

**Issue: "Pipeline fails on specific files"**
```
Solution: Add file-specific error handling
- Check logs for specific filename
- Add try-except for that file in the pipeline
- Consider excluding problematic files temporarily
```

### Debugging

1. **Check Pipeline Logs**:
   - Open DLT pipeline
   - View "Event Log" tab
   - Look for errors and warnings

2. **Test Individual Functions**:
   - Run notebook cells individually
   - Test helper functions with specific files
   - Use `display()` to inspect DataFrames

3. **Validate Data**:
   ```sql
   -- Check row counts
   SELECT COUNT(*) FROM main.bls_data.pr_data_0_current;
   
   -- Check for nulls
   SELECT COUNT(*) 
   FROM main.bls_data.pr_data_0_current 
   WHERE series_id IS NULL;
   
   -- Check latest ingestion
   SELECT MAX(ingestion_timestamp) 
   FROM main.bls_data.pr_data_0_current;
   ```

## Maintenance

### Regular Tasks
- **Monitor Pipeline Runs**: Check for failures weekly
- **Review Data Quality**: Monitor expectation metrics monthly
- **Update User-Agent**: Keep contact info current
- **Optimize Tables**: Run `OPTIMIZE` on large tables quarterly

### Optimization Commands
```sql
-- Optimize tables for better query performance
OPTIMIZE main.bls_data.pr_data_0_current
ZORDER BY series_id;

-- Vacuum old versions (after 7 days retention)
VACUUM main.bls_data.pr_data_0_current RETAIN 168 HOURS;

-- Update table statistics
ANALYZE TABLE main.bls_data.pr_data_0_current COMPUTE STATISTICS;
```

## Support and Resources

- **Databricks Documentation**: https://docs.databricks.com/workflows/delta-live-tables/
- **Delta Lake Documentation**: https://docs.delta.io/
- **Unity Catalog Guide**: https://docs.databricks.com/data-governance/unity-catalog/
- **BLS Data Information**: https://www.bls.gov/bls/pss.htm

## License

This implementation follows the same license as the parent repository.

## Contributing

To contribute improvements:
1. Test changes in development mode
2. Update documentation
3. Submit pull request with clear description
