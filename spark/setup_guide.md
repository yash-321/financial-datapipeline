# Spark + Iceberg Local Setup Guide

This guide walks you through setting up your local machine to run the `landing_to_bronze.ipynb` notebook for learning Spark and Iceberg.

## Prerequisites

- **Python:** 3.9 or later (3.10+ recommended)
- **Java:** JDK 11 or later (Spark requires Java)
- **pip:** Python package manager
- **AWS CLI:** (optional, but helpful for S3 testing)

## Step 1: Verify Java Installation

Spark requires Java to run. Check if you have it:

```bash
java -version
```

Expected output:
```
openjdk version "11.0.20" 2024-01-16
```

### If Java is not installed:

**On macOS (Homebrew):**
```bash
brew install openjdk@11
export PATH="/usr/local/opt/openjdk@11/bin:$PATH"
```

**On Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install openjdk-11-jdk
```

**On Windows:**
- Download from [adoptium.net](https://adoptium.net/)
- Add to PATH (usually automatic)

## Step 2: Verify Python Version

```bash
python3 --version
```

Should be 3.9 or later. If not, install Python 3.10+:

```bash
# macOS
brew install python@3.10

# Ubuntu
sudo apt-get install python3.10

# Windows
# Download from python.org
```

## Step 3: Install Spark + Iceberg Dependencies

You should be in the DataPipeline project root. The dependencies are already in `spark/requirements.txt`:

```bash
# Navigate to spark directory
cd /home/yash/home-stuff/DataPipeline

# Install dependencies (one-time)
pip install -r spark/requirements.txt
```

This installs:
- **pyspark==3.5.0** — Apache Spark with Python bindings
- **pyarrow==14.0.0** — Apache Arrow for efficient data transfer
- **pyiceberg==0.6.1** — Apache Iceberg client library
- **boto3==1.28.0** — AWS SDK for S3 access
- **python-dotenv==1.0.0** — Load environment variables from `.env`

### Verify Installation

```bash
python3 -c "import pyspark; print(pyspark.__version__)"
python3 -c "import pyiceberg; print(pyiceberg.__version__)"
python3 -c "import boto3; print(boto3.__version__)"
```

## Step 4: Install and Configure Jupyter

### Install Jupyter Notebook

```bash
pip install jupyter notebook>=7.0
```

### Verify Installation

```bash
jupyter notebook --version
```

## Step 5: Configure AWS Credentials

The notebook reads S3 data from your landing bucket. You need AWS credentials.

### Option A: Use `.env` file (Recommended for local development)

1. **Copy `.env.example` to `.env`:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your credentials:**
   ```bash
   # Open .env in your editor and fill in:
   AWS_ACCESS_KEY_ID=your_access_key_id
   AWS_SECRET_ACCESS_KEY=your_secret_access_key
   AWS_REGION=eu-west-1
   S3_BUCKET=financial-data-landing-587129419094-eu-west-1-an
   ```

3. **The notebook will load these automatically** using `python-dotenv`.

### Option B: Use AWS CLI Profile

If you already have AWS CLI configured:

```bash
# List existing profiles
aws configure list-profiles

# The notebook can read from your default profile or specify a profile name
```

### Option C: Use Environment Variables

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=eu-west-1
```

### Verify S3 Access

Test that your credentials work:

```bash
aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/trades/ --region eu-west-1
```

You should see date-partitioned folders:
```
                           PRE date=2026-05-09/
                           PRE date=2026-08-19/
```

If this fails, check:
- ✅ AWS_ACCESS_KEY_ID is correct
- ✅ AWS_SECRET_ACCESS_KEY is correct
- ✅ Your credentials have S3 permissions (`s3:GetObject`, `s3:ListBucket`)
- ✅ AWS_REGION matches the bucket region

## Step 6: Set Up Iceberg Catalog (Local SQLite)

For this learning exercise, we'll use a **local SQLite Iceberg catalog** (simplest for development).

The notebook handles this automatically, but here's what it does:

```python
from pyiceberg.catalog import load_catalog

# Create local catalog (auto-creates)
catalog = load_catalog(
    "local",
    type="hive",
    warehouse="./warehouse"  # Local directory for metadata
)
```

This creates a `warehouse/` folder in your project to store Iceberg metadata.

### If using AWS Glue Catalog (Optional - Production Setup)

See `spark/glue_catalog_setup.md` for details on AWS Glue Catalog configuration.

## Step 7: Prepare Local Directory Structure

The notebook will create these folders automatically, but you can prepare them:

```bash
# From DataPipeline root
mkdir -p warehouse/{metadata,data}
mkdir -p notebooks_output
```

## Step 8: Launch Jupyter Notebook

```bash
# From DataPipeline root
jupyter notebook

# Or specify the spark directory
jupyter notebook spark/
```

Jupyter will open in your browser at `http://localhost:8888`.

### Create/Open the Notebook

1. Click **New** → **Python 3** to create a new notebook
2. OR open existing `spark/landing_to_bronze.ipynb`
3. Set kernel to **Python 3**

## Step 9: Configure Spark Session in Notebook

The notebook initializes Spark with Iceberg support automatically. First cell:

```python
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load .env credentials
load_dotenv()

# Create Spark session with Iceberg catalog
spark = SparkSession.builder \
    .appName("landing-to-bronze") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.SparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hive") \
    .config("spark.sql.catalog.local.warehouse", "./warehouse") \
    .getOrCreate()

print(f"✅ Spark {spark.version} session created successfully!")
```

## Step 10: Run the First Cell

In the notebook, run the first cell to verify everything works:

```python
# Test Spark + Iceberg
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW CATALOGS").show()
```

Expected output:
```
+--------+
|database|
+--------+
| default|
+--------+

+-------+
|catalog|
+-------+
|  local|
+-------+
```

## Troubleshooting Setup

### Java not found
```
Error: java: command not found
```
**Fix:** Install Java (see Step 1) and add to PATH

### PySpark import error
```
ModuleNotFoundError: No module named 'pyspark'
```
**Fix:** Run `pip install -r spark/requirements.txt` in the DataPipeline directory

### S3 credentials not found
```
NoCredentialsError: Unable to locate credentials
```
**Fix:**
- Check `.env` file exists with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Verify file is in DataPipeline root
- Restart Jupyter after changing `.env`

### "Cannot access S3 bucket"
```
An error occurred (AccessDenied) when calling the GetObject operation: Access Denied
```
**Fix:**
- Verify credentials have S3 permissions
- Check AWS_REGION matches bucket region (eu-west-1)
- Test with: `aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/ --region eu-west-1`

### Jupyter not starting
```
Error: No module named 'jupyter'
```
**Fix:** Run `pip install jupyter notebook`

### Out of memory errors
```
java.lang.OutOfMemoryError: Java heap space
```
**Fix:** Increase Spark driver memory:
```python
spark.conf.set("spark.driver.memory", "4g")
# or configure in SparkSession.builder()
```

## Verification Checklist

Before starting the notebook, verify:

- [ ] Java installed: `java -version`
- [ ] Python 3.9+: `python3 --version`
- [ ] PySpark installed: `python3 -c "import pyspark"`
- [ ] Jupyter installed: `jupyter notebook --version`
- [ ] AWS credentials in `.env`: cat `.env | grep AWS_`
- [ ] S3 access works: `aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/trades/`
- [ ] Jupyter starts: `jupyter notebook` (can kill with Ctrl+C)

## Quick Reference

```bash
# Install all dependencies
pip install -r spark/requirements.txt jupyter

# Start Jupyter
jupyter notebook spark/

# View logs (in case of errors)
tail -f ~/spark_logs.txt

# Reset Iceberg warehouse (start fresh)
rm -rf warehouse/

# Test Spark + S3
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.parquet('s3a://financial-data-landing-587129419094-eu-west-1-an/trades/date=2026-05-09/symbol=AAPL/')
print(f'✅ Read {df.count()} records from S3')
"
```

## Next Steps

Once setup is complete:

1. Open `jupyter notebook spark/`
2. Create or open `landing_to_bronze.ipynb`
3. Follow the 12-section notebook step-by-step
4. Use `spark/troubleshooting_guide.md` if you hit errors
5. Refer to `MEMORY.md` for project architecture context

## Additional Resources

- [Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-getting-started.html)
- [PySpark API Docs](https://spark.apache.org/docs/latest/api/python/)
- [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/)
- [PyIceberg Python Client](https://py.iceberg.apache.org/)
- [AWS Glue Catalog Integration](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
