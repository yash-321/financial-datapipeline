# AWS Glue Catalog Setup for Iceberg (Optional - Production)

This guide covers setting up AWS Glue Catalog with Apache Iceberg for production use. 

**Note:** For the learning notebook, we use a **local SQLite Iceberg catalog** (simpler, no AWS required). This guide is for when you're ready to deploy to production.

## When to Use Glue Catalog vs Local Catalog

### Local Catalog (Development/Learning)
```python
catalog = load_catalog("local", type="hive", warehouse="./warehouse")
```
✅ **Use for:**
- Local development and testing
- Learning (this notebook)
- CI/CD pipelines in isolated environments
- No AWS credentials needed

❌ **Limitations:**
- Only accessible locally
- No multi-machine sharing
- No RBAC (access control)
- Single warehouse per machine

### AWS Glue Catalog (Production)
```python
catalog = load_catalog("glue", type="glue", warehouse="s3://bucket/warehouse")
```
✅ **Use for:**
- Shared team environments
- Multi-machine/multi-service access
- AWS native infrastructure
- Cross-service integration (EMR, Lambda, SageMaker)
- Centralized metadata management

### Other Options
- **Nessie Catalog:** For time-travel + versioning
- **Delta Lake Metadata:** For Databricks compatibility
- **Hive Metastore:** For on-premise Hadoop clusters

## AWS Glue Catalog Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS Glue Catalog                         │
│  (Centralized Metadata Repository for Tables & Databases)   │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─ Databases (namespaces)
             │   └─ Tables (metadata pointers)
             │       ├─ Schema
             │       ├─ Location (S3 path)
             │       ├─ Partitions
             │       └─ Properties
             │
             └─ Iceberg Metadata
                 ├─ Snapshots (versions)
                 ├─ Manifests (file lists)
                 └─ Metadata files (v1.json, v2.json, ...)

Actual Data Storage:
s3://your-warehouse-bucket/
├── warehouse/
│   ├── database_name/
│   │   └── table_name/
│   │       ├── metadata/
│   │       │   ├── v1.metadata.json
│   │       │   └── ...
│   │       ├── data/
│   │       │   ├── 00001.parquet
│   │       │   └── ...
```

## Prerequisites

- ✅ AWS Account with permissions
- ✅ AWS CLI installed and configured
- ✅ S3 bucket for warehouse data
- ✅ IAM role/policy for Glue access
- ✅ PySpark with Iceberg libraries
- ✅ pyiceberg library installed

## Step 1: Create S3 Warehouse Bucket

```bash
# Create bucket (choose unique name)
aws s3api create-bucket \
    --bucket financial-data-warehouse-587129419094-eu-west-1 \
    --region eu-west-1 \
    --create-bucket-configuration LocationConstraint=eu-west-1

# Enable versioning (good for data recovery)
aws s3api put-bucket-versioning \
    --bucket financial-data-warehouse-587129419094-eu-west-1 \
    --versioning-configuration Status=Enabled

# Block public access (security)
aws s3api put-public-access-block \
    --bucket financial-data-warehouse-587129419094-eu-west-1 \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# List to verify
aws s3 ls | grep warehouse
```

## Step 2: Create Glue Database

```bash
# Create database in AWS Glue Catalog
aws glue create-database \
    --database-input '{
        "Name": "analytics_bronze",
        "Description": "Bronze layer: raw data from landing bucket"
    }' \
    --region eu-west-1

# List databases
aws glue get-databases --region eu-west-1
```

## Step 3: Configure IAM Permissions

Your Spark job needs these S3 + Glue permissions.

### Create IAM Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueReadWrite",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:PutDataCatalogEncryptionSettings",
                "glue:GetDataCatalogEncryptionSettings"
            ],
            "Resource": "arn:aws:glue:eu-west-1:587129419094:*"
        },
        {
            "Sid": "S3WarehouseAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::financial-data-warehouse-587129419094-eu-west-1/*",
                "arn:aws:s3:::financial-data-warehouse-587129419094-eu-west-1"
            ]
        },
        {
            "Sid": "S3LandingAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::financial-data-landing-587129419094-eu-west-1-an/*",
                "arn:aws:s3:::financial-data-landing-587129419094-eu-west-1-an"
            ]
        },
        {
            "Sid": "KmsDecrypt",
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt",
                "kms:GenerateDataKey",
                "kms:DescribeKey"
            ],
            "Resource": "arn:aws:kms:eu-west-1:587129419094:key/*"
        }
    ]
}
```

### Attach Policy to Role

```bash
# If using EC2 instance role:
aws iam attach-role-policy \
    --role-name spark-glue-role \
    --policy-arn arn:aws:iam::aws:policy/CustomSparkGluePolicy

# If using IAM user (not recommended for production):
aws iam attach-user-policy \
    --user-name spark-user \
    --policy-arn arn:aws:iam::aws:policy/CustomSparkGluePolicy
```

## Step 4: Configure Spark for Glue Catalog

```python
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("landing-to-bronze") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1") \
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.SparkSessionExtensions") \
    \
    # AWS Glue Catalog configuration\
    .config("spark.sql.catalog.glue_catalog", 
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", 
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", 
            "s3://financial-data-warehouse-587129419094-eu-west-1/warehouse") \
    .config("spark.sql.catalog.glue_catalog.io-impl", 
            "org.apache.iceberg.aws.s3.S3FileIO") \
    \
    # S3 credentials (from .env or IAM role)\
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID', '')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY', '')) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com") \
    \
    # Optimize for Glue\
    .config("spark.sql.catalog.glue_catalog.cache-enabled", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "30000") \
    \
    .getOrCreate()

print("✅ Spark configured for AWS Glue Catalog")
```

### Alternative: Use IAM Role (EC2/EMR)

If running on EC2/EMR with IAM role (no explicit credentials needed):

```python
spark = SparkSession.builder \
    .appName("landing-to-bronze") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1") \
    .config("spark.sql.catalog.glue_catalog", 
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", 
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", 
            "s3://financial-data-warehouse-587129419094-eu-west-1/warehouse") \
    # No explicit AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY needed\
    # IAM role credentials used automatically\
    .getOrCreate()

print("✅ Using IAM role for AWS authentication")
```

## Step 5: Create Iceberg Table in Glue

```python
# Create table in Glue Catalog
catalog_name = "glue_catalog"
database_name = "analytics_bronze"
table_name = "raw_trades"

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{database_name}.{table_name} (
    symbol STRING NOT NULL,
    trade_id STRING NOT NULL,
    price DOUBLE NOT NULL,
    quantity DOUBLE NOT NULL,
    event_timestamp LONG NOT NULL,
    ingestion_timestamp LONG NOT NULL,
    date STRING NOT NULL
)
USING iceberg
PARTITIONED BY (date, symbol)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
"""

spark.sql(create_table_sql)
print(f"✅ Created table: {catalog_name}.{database_name}.{table_name}")

# Verify in Glue
spark.sql(f"SHOW TABLES IN {catalog_name}.{database_name}").show()
```

## Step 6: Verify Glue Setup

```bash
# List Glue databases
aws glue get-databases --region eu-west-1

# List Glue tables in a database
aws glue get-tables \
    --database-name analytics_bronze \
    --region eu-west-1

# Check S3 warehouse structure
aws s3 ls s3://financial-data-warehouse-587129419094-eu-west-1/warehouse/ --recursive
```

## Step 7: Query via Glue Catalog

Once created, query the table:

```python
# Query from Glue Catalog
df = spark.sql(f"""
    SELECT * 
    FROM glue_catalog.analytics_bronze.raw_trades
    WHERE date = '2026-05-09'
    LIMIT 10
""")

df.show()

# Register as temp view for easier access
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW trades_bronze AS
    SELECT * FROM glue_catalog.analytics_bronze.raw_trades
""")

# Now use simpler name
spark.sql("SELECT COUNT(*) as count FROM trades_bronze").show()
```

## Step 8: Cross-Service Access

Once in Glue, other AWS services can access your Iceberg table:

### AWS Lambda
```python
import boto3
from pyspark.sql import SparkSession

# Lambda can query Glue Catalog too
glue_client = boto3.client('glue')
tables = glue_client.get_tables(DatabaseName='analytics_bronze')
print(tables)
```

### AWS Athena
```sql
-- Query Iceberg table directly from Athena
SELECT * 
FROM "analytics_bronze"."raw_trades"
WHERE date = '2026-05-09'
LIMIT 10;
```

### Amazon SageMaker
```python
import sagemaker.spark

spark = sagemaker.spark.get_spark_session()

df = spark.sql("""
    SELECT * 
    FROM glue_catalog.analytics_bronze.raw_trades
""")

# Use for ML training
df.to_pandas()
```

## Common Issues & Troubleshooting

### Issue: "Glue catalog not found"
```
org.apache.iceberg.exceptions.NoSuchCatalogException: Could not load catalog
```
**Solution:**
- Verify Glue database exists: `aws glue get-databases`
- Check IAM permissions (GlueReadWrite policy attached)
- Verify S3 warehouse path is correct
- Check Spark configuration (catalog-impl, warehouse)

### Issue: "Permission denied" on S3
```
com.amazon.aws.crt.s3.S3Exception: Access Denied
```
**Solution:**
- Verify IAM policy has S3 bucket ARN
- Check bucket name spelling
- Ensure region matches
- Test with: `aws s3 ls s3://bucket-name`

### Issue: "Table metadata not found"
```
org.apache.iceberg.exceptions.NotFoundException: Table metadata not found
```
**Solution:**
- Table might be in different database
- Check table name: `SHOW TABLES IN glue_catalog.database_name`
- Verify Iceberg table was created (not Hive table)

### Issue: Slow queries on Glue
```
Queries taking longer than expected
```
**Solution:**
- Enable Glue cache: `spark.sql.catalog.glue_catalog.cache-enabled = true`
- Use partition filtering: `WHERE date = '2026-05-09'`
- Increase executor memory: `spark.executor.memory = 8g`

## Best Practices

1. **Separate Databases:** Use different databases for Bronze/Silver/Gold
   ```sql
   -- Bronze (raw)
   analytics_bronze.raw_trades
   
   -- Silver (cleaned)
   analytics_silver.trades
   
   -- Gold (aggregated)
   analytics_gold.daily_ohlcv
   ```

2. **Use IAM Roles:** Don't embed credentials in code (use EC2/EMR roles)

3. **Enable Encryption:** Use KMS for S3 + Glue metadata

4. **Partition Strategy:** Align Iceberg partitions with query patterns

5. **Monitor Cost:** S3 list operations can be expensive at scale
   - Use partition pruning heavily
   - Cache frequently accessed metadata
   - Archive old snapshots

6. **Version Control:** Tag metadata versions for audit trail

## Production Deployment Checklist

- [ ] S3 warehouse bucket created and versioned
- [ ] Glue database created
- [ ] IAM role/policy created and attached
- [ ] Spark configuration validated
- [ ] Table created in Glue Catalog
- [ ] Sample data written and readable
- [ ] Query performance validated
- [ ] Cross-service access tested (Athena, Lambda)
- [ ] Backup/disaster recovery plan
- [ ] Monitoring and alerting configured
- [ ] Cost estimation completed

## Next Steps

1. **Local Development:** Use local SQLite catalog (this notebook)
2. **Staging:** Deploy to Glue Catalog in dev AWS account
3. **Load Testing:** Test with realistic data volumes
4. **Production:** Deploy to prod with proper RBAC and encryption

## Additional Resources

- [AWS Glue Catalog Documentation](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Apache Iceberg AWS Integration](https://iceberg.apache.org/docs/latest/aws/)
- [PyIceberg Glue Catalog](https://py.iceberg.apache.org/generated/pyiceberg.catalogs.GlueCatalog.html)
- [AWS Glue Best Practices](https://docs.aws.amazon.com/glue/latest/dg/best-practices.html)
