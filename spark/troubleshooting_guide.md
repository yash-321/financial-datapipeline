# Spark + Iceberg Troubleshooting Guide

A comprehensive guide for debugging common issues when running the `landing_to_bronze.ipynb` notebook.

## Quick Diagnosis Flowchart

```
Error occurs in notebook
    ↓
Is it a Spark error? → See: Spark Errors
    ↓
Is it an S3/AWS error? → See: S3 & AWS Errors
    ↓
Is it an Iceberg error? → See: Iceberg Errors
    ↓
Is it a memory/performance issue? → See: Performance Issues
    ↓
Still stuck? → See: General Debugging Tips
```

---

## 1. Spark Errors

### Error: "Java not found" or "JAVA_HOME not set"

```
Error: java: command not found
```

**Cause:** Java not installed or not in PATH

**Solution:**
```bash
# Check if Java is installed
java -version

# If not found, install
# macOS:
brew install openjdk@11

# Ubuntu/Debian:
sudo apt-get install openjdk-11-jdk

# Windows:
# Download from adoptium.net and add to PATH

# Set JAVA_HOME (add to ~/.bashrc or ~/.zshrc)
export JAVA_HOME=/usr/libexec/java_home -v 11
```

**Verify:**
```bash
java -version
echo $JAVA_HOME
```

---

### Error: "ModuleNotFoundError: No module named 'pyspark'"

```
ModuleNotFoundError: No module named 'pyspark'
```

**Cause:** PySpark not installed or wrong Python environment

**Solution:**
```bash
# Install dependencies
cd /home/yash/home-stuff/DataPipeline
pip install -r spark/requirements.txt

# Verify installation
python3 -c "import pyspark; print(pyspark.__version__)"
```

**Advanced:**
```bash
# Check which Python pip is using
which pip
which python3

# Might need to use python3 explicitly
python3 -m pip install pyspark==3.5.0

# Verify in same Python
python3 -c "import pyspark; print(pyspark.__version__)"
```

---

### Error: "Py4JJavaError" or "Cannot find spark"

```
Py4JJavaError: An error occurred while calling ...
```

**Cause:** Spark session initialization failed, Java/Scala version mismatch

**Solution:**
```bash
# Check Spark is installed with PySpark
python3 -c "from pyspark import SparkContext; print(SparkContext._jvm)"

# Check Java version compatibility (need 11+)
java -version

# Try creating SparkSession in Python shell
python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(f"Spark {spark.version}")
spark.stop()
EOF
```

---

### Error: "OutOfMemoryError: Java heap space"

```
java.lang.OutOfMemoryError: Java heap space
```

**Cause:** Spark driver or executor doesn't have enough memory

**Solution:**
```python
# In notebook, before creating SparkSession:
spark = SparkSession.builder \
    .appName("landing-to-bronze") \
    .config("spark.driver.memory", "8g") \  # Increase from 4g
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Alternative: Set environment variable before starting notebook
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=4g
jupyter notebook
```

**Debugging:**
```python
# Check current memory allocation
spark.conf.get("spark.driver.memory")
spark.conf.get("spark.executor.memory")

# Reduce data size being processed
df_small = spark.sql("SELECT * FROM trades_landing LIMIT 10000")
```

---

### Error: "Target already exists" or "Table already exists"

```
org.apache.spark.sql.AnalysisException: path s3a://...warehouse/... already exists
```

**Cause:** Writing to path that already has a table

**Solution:**
```python
# Option 1: Use 'overwrite' mode (replace existing)
df.write.format("iceberg").mode("overwrite").saveAsTable("table_name")

# Option 2: Drop table first
spark.sql("DROP TABLE IF EXISTS local.default.raw_trades")

# Option 3: Use 'append' mode (add to existing)
df.write.format("iceberg").mode("append").saveAsTable("table_name")
```

---

## 2. S3 & AWS Errors

### Error: "Unable to locate credentials"

```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Cause:** AWS credentials not found in environment

**Solution:**

**Option A: .env File**
```bash
# Create/check .env file exists
ls -la /home/yash/home-stuff/DataPipeline/.env

# Content should have:
# AWS_ACCESS_KEY_ID=your_key
# AWS_SECRET_ACCESS_KEY=your_secret
# AWS_REGION=eu-west-1

# In Python, verify load:
from dotenv import load_dotenv
import os
load_dotenv()
print(os.getenv('AWS_ACCESS_KEY_ID'))  # Should print your key
```

**Option B: AWS CLI Profile**
```bash
# Check if credentials configured
aws configure list

# List available profiles
aws configure list-profiles

# Use specific profile
export AWS_PROFILE=default
```

**Option C: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=eu-west-1

# Start notebook
jupyter notebook
```

---

### Error: "An error occurred (AccessDenied)"

```
botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling GetObject
```

**Cause:** Credentials exist but lack S3 permissions

**Solution:**
```bash
# Test credentials work
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/trades/ \
    --region eu-west-1

# If error: permissions issue
# Check with AWS admin:
# - Do credentials have S3:GetObject permission?
# - Do credentials have s3:ListBucket permission?
# - Is bucket name correct?
# - Is region correct?
```

**IAM Policy Required:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::financial-data-landing-587129419094-eu-west-1-an/*",
                "arn:aws:s3:::financial-data-landing-587129419094-eu-west-1-an"
            ]
        }
    ]
}
```

---

### Error: "The specified bucket does not exist"

```
botocore.exceptions.ClientError: ... (NoSuchBucket)
```

**Cause:** Bucket name is wrong or doesn't exist

**Solution:**
```bash
# List all buckets you have access to
aws s3 ls

# Check exact bucket name
aws s3 ls | grep financial

# Verify in notebook
bucket = "financial-data-landing-587129419094-eu-west-1-an"
s3_client = boto3.client('s3', region_name='eu-west-1')
s3_client.head_bucket(Bucket=bucket)
print("✅ Bucket exists and accessible")
```

---

### Error: "The bucket may not exist"

```
FileNotFoundError: The bucket may not exist
```

**Cause:** S3 path issue (prefix doesn't exist or empty)

**Solution:**
```bash
# List specific path
aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/trades/

# If empty, check if data was written
aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/

# Check with producer that data is being written
cd /home/yash/home-stuff/DataPipeline
docker-compose up trade-consumer
# Wait for 30 seconds for data to flush
```

---

### Error: "Slow S3 operations" (Not an error, but slow)

**Symptom:** Operations like `spark.read.parquet()` take > 1 minute

**Cause:** S3 list operations are slow, network latency

**Solution:**
```python
# Use basePath hint to skip discovery
df = spark.read \
    .option("basePath", "s3a://bucket/trades/") \
    .parquet("s3a://bucket/trades/")

# Partition pruning: only read what you need
df_filtered = df.filter("date = '2026-05-09'")  # Spark skips other dates

# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "16")

# Cache if reading multiple times
df.cache()
df.count()  # Trigger cache
```

---

## 3. Iceberg Errors

### Error: "Cannot find Iceberg Spark extensions"

```
org.apache.spark.sql.AnalysisException: 
cannot use the Iceberg Spark SQL extension
```

**Cause:** Iceberg JAR not loaded by Spark

**Solution:**
```python
# Verify JAR is included in config
spark = SparkSession.builder \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1") \  # ← Must be present
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.SparkSessionExtensions") \  # ← Must be present
    .getOrCreate()

# Test Iceberg is loaded
spark.sql("SHOW CATALOGS").show()  # Should show 'local' catalog
```

---

### Error: "Table metadata not found"

```
org.apache.iceberg.exceptions.NotFoundException: 
Table metadata not found at /path/to/warehouse/...
```

**Cause:** Table doesn't exist or wrong path

**Solution:**
```python
# Check if table exists
spark.sql("SHOW TABLES IN local.default").show()

# Check warehouse directory
import os
if os.path.exists("./warehouse"):
    print("Warehouse exists")
    os.system("find ./warehouse -name '*.json' | head -10")
else:
    print("Warehouse not found - create table first")

# Recreate if necessary
spark.sql("DROP TABLE IF EXISTS local.default.raw_trades")
# Then re-run Section 6 of notebook
```

---

### Error: "Schema mismatch" or "Cannot cast"

```
org.apache.spark.sql.AnalysisException: 
cannot cast ... because data type mismatch
```

**Cause:** DataFrame schema doesn't match table schema

**Solution:**
```python
# Check DataFrame schema
df_landing.printSchema()

# Check table schema
spark.sql("DESCRIBE TABLE local.default.raw_trades").show()

# Align schemas before writing
required_cols = ['symbol', 'trade_id', 'price', 'quantity', 'event_timestamp', 'ingestion_timestamp', 'date']
df_clean = df_landing.select(required_cols)

# Cast columns to correct types if needed
from pyspark.sql.types import DoubleType, LongType
df_clean = df_clean \
    .withColumn("price", df_clean["price"].cast(DoubleType())) \
    .withColumn("quantity", df_clean["quantity"].cast(DoubleType())) \
    .withColumn("event_timestamp", df_clean["event_timestamp"].cast(LongType()))

df_clean.printSchema()  # Verify
```

---

### Error: "Partition spec not found"

```
org.apache.iceberg.exceptions.ValidationException:
Partition spec not found
```

**Cause:** Table not properly partitioned

**Solution:**
```python
# When creating table, include PARTITIONED BY
create_sql = """
CREATE TABLE IF NOT EXISTS local.default.raw_trades (
    symbol STRING,
    trade_id STRING,
    price DOUBLE,
    quantity DOUBLE,
    event_timestamp LONG,
    ingestion_timestamp LONG,
    date STRING
)
USING iceberg
PARTITIONED BY (date, symbol)  # ← Must be present
"""
spark.sql(create_sql)
```

---

### Error: "Unsupported write mode"

```
java.lang.UnsupportedOperationException: 
Unsupported write mode: append_only
```

**Cause:** Iceberg version doesn't support certain write modes

**Solution:**
```python
# Use standard modes: 'overwrite', 'append', 'ignore', 'error'
df.write \
    .format("iceberg") \
    .mode("overwrite") \  # ← Use one of these
    .saveAsTable("table_name")

# Check Iceberg version
spark.sql("SELECT spark_version()").show()
```

---

## 4. Data-Related Errors

### Error: "0 records read from S3"

```
Total records: 0
```

**Cause:** Landing data doesn't exist or wrong path

**Solution:**
```bash
# Verify data exists in landing bucket
aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/trades/

# If empty, start the producer + consumer
cd /home/yash/home-stuff/DataPipeline
docker-compose up

# In another terminal:
make up-consumers

# Wait 30-60 seconds for data to be written

# Check again
aws s3 ls s3://financial-data-landing-587129419094-eu-west-1-an/trades/
```

---

### Error: "Null values found in required fields"

```
Validation failed: Found NULL trade_ids
```

**Cause:** Data quality issue upstream

**Solution:**
```python
# Option 1: Filter out nulls
df_clean = df_landing.filter("trade_id IS NOT NULL")

# Option 2: Investigate null source
spark.sql("""
    SELECT COUNT(*) as null_count
    FROM trades_landing
    WHERE trade_id IS NULL
""").show()

# Option 3: Fill nulls with default
from pyspark.sql.functions import coalesce, lit
df_clean = df_landing.withColumn(
    "trade_id",
    coalesce(col("trade_id"), lit("UNKNOWN"))
)
```

---

### Error: "Duplicate records found"

```
Validation failed: Found duplicate trade_ids
```

**Cause:** Producer retry logic created duplicates (intentional for testing)

**Solution:**
```python
# Deduplicate: keep most recent
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window = Window.partitionBy("trade_id").orderBy(desc("ingestion_timestamp"))
df_dedup = df_landing \
    .withColumn("rn", row_number().over(window)) \
    .filter("rn = 1") \
    .drop("rn")

# Verify duplicates removed
before = df_landing.select("trade_id").distinct().count()
after = df_dedup.select("trade_id").distinct().count()
print(f"Before: {before}, After: {after}")
```

---

## 5. Performance Issues

### Issue: "Notebook runs very slowly"

**Symptom:** Cells take > 5 minutes to execute

**Cause:** Too much data, network latency, memory pressure

**Solution:**

**1. Profile the job:**
```python
# See which operation is slow
import time

start = time.time()
df = spark.read.parquet("s3a://...")
print(f"Read time: {time.time() - start}s")

start = time.time()
count = df.count()
print(f"Count time: {time.time() - start}s")

start = time.time()
df.write.format("iceberg").mode("overwrite").saveAsTable("table")
print(f"Write time: {time.time() - start}s")
```

**2. Filter data early:**
```python
# Instead of reading all, filter for single date
df = spark.sql("""
    SELECT * FROM trades_landing
    WHERE date = '2026-05-09'  # Small subset
    LIMIT 10000
""")
```

**3. Repartition for parallelism:**
```python
# More partitions = more parallel tasks
df = df.repartition(16)  # Adjust based on CPU cores
```

**4. Use caching:**
```python
# Cache data if accessed multiple times
df.cache()
df.count()  # Trigger cache into memory
```

---

### Issue: "Spark UI shows 100% GC time"

**Symptom:** Slow progress, lots of garbage collection

**Cause:** Not enough memory, too many objects

**Solution:**
```python
# Increase executor memory
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

# Or reduce data volume
df = spark.sql("SELECT * FROM trades_landing LIMIT 100000")
```

---

## 6. General Debugging Tips

### Enable Spark Logging

```python
# Set log level
spark.sparkContext.setLogLevel("INFO")  # or "DEBUG" for more details

# See Spark logs
# Option A: In notebook
from pyspark.sql import SparkSession
spark.sparkContext.setLogLevel("DEBUG")

# Option B: View Spark UI
# Open browser: http://localhost:4040
# Shows jobs, stages, executors, logs
```

### Check Spark UI

```
While notebook is running:
1. Open: http://localhost:4040
2. View:
   - Jobs: What's currently running
   - Stages: How many stages in pipeline
   - Executors: Memory/cores available
   - SQL: Query execution plans
   - Logs: Driver logs
```

### Print Intermediate Results

```python
# Don't just check final result
df1 = spark.read.parquet("s3a://...")
print(f"After read: {df1.count()} rows")

df2 = df1.filter("price > 0")
print(f"After filter: {df2.count()} rows")

df3 = df2.select("symbol", "price")
print(f"After select: {df3.count()} rows")
df3.show(5)  # Peek at data
```

### Use explain() for query plans

```python
# Understand how Spark will execute query
df.explain(mode="simple")  # Simple plan
df.explain(mode="extended")  # Detailed plan with costs
```

### Test with smaller data

```python
# When debugging, use smaller sample
df_small = spark.sql("""
    SELECT * FROM trades_landing
    LIMIT 1000
""")

# Run transformations on small data
df_small.write.format("iceberg").mode("overwrite").saveAsTable("test_table")

# Verify before running on full data
spark.sql("SELECT COUNT(*) FROM test_table").show()
```

---

## 7. Common Issue Combinations

### Slow + "OutOfMemory" errors

```
java.lang.OutOfMemoryError: Java heap space
(after running for 5+ minutes)
```

**Solution:**
```python
# Increase memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Process smaller chunks
for date in ['2026-05-09', '2026-08-19']:
    df = spark.sql(f"SELECT * FROM trades_landing WHERE date = '{date}'")
    df.write.format("iceberg").mode("append").saveAsTable("raw_trades")
```

### "AccessDenied" + "FileNotFound"

```
botocore.exceptions.ClientError: (AccessDenied)
FileNotFoundError: The bucket may not exist
```

**Solution:**
```bash
# Is it access or bucket?
# Test directly:
aws s3 ls s3://bucket-name/path/

# If that fails → access issue
# Check credentials: aws sts get-caller-identity

# If access OK but still fails → wrong path
# List what exists: aws s3 ls s3://bucket-name/ --recursive
```

---

## 8. Asking for Help

When posting in forums/Slack, include:

```markdown
**Error:**
```
[full error message]
```

**Code that caused it:**
```python
[relevant code cell]
```

**Environment:**
- OS: macOS / Linux / Windows
- Python version: `python3 --version`
- PySpark version: `python3 -c "import pyspark; print(pyspark.__version__)"`
- AWS region: eu-west-1
- Have credentials: yes / no

**Steps to reproduce:**
1. ...
2. ...

**What I've tried:**
- Checked .env file
- Restarted Jupyter
- ...
```

---

## Quick Reference: Common Fixes

| Error | Quick Fix |
|-------|-----------|
| `ModuleNotFoundError: pyspark` | `pip install -r spark/requirements.txt` |
| `java: command not found` | Install Java 11+ |
| `NoCredentialsError` | Check `.env` file exists with AWS_ACCESS_KEY_ID |
| `AccessDenied` | Test: `aws sts get-caller-identity` |
| `OutOfMemoryError` | Increase: `spark.driver.memory = 8g` |
| `Table not found` | Drop & recreate: `DROP TABLE IF EXISTS ...` |
| `Null values in data` | Filter: `df.filter("column IS NOT NULL")` |
| `Slow queries` | Add partition filter: `WHERE date = '...'` |

---

## Need More Help?

1. **Check the main notebook:** [landing_to_bronze.ipynb](landing_to_bronze.ipynb)
2. **Setup guide:** [setup_guide.md](setup_guide.md)
3. **Glue Catalog guide:** [glue_catalog_setup.md](glue_catalog_setup.md)
4. **Project memory:** [../MEMORY.md](../MEMORY.md)
5. **Spark docs:** https://spark.apache.org/docs/
6. **Iceberg docs:** https://iceberg.apache.org/docs/latest/
