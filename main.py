from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, regexp_replace, regexp_extract, udf, count, count_distinct, when, sum as sum_, max as max_
from pyspark.sql.types import StringType
import os 

# Set JAVA_HOME for PySpark 
os.environ["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@11/11.0.26/libexec/openjdk.jdk/Contents/Home"

# Initialize Spark session
spark = SparkSession.builder \
        .appName("SemiconductorSupplyChainETL") \
        .getOrCreate()


# Load CSVs into Spark DataFrames
inputs_df = spark.read.option("header", True) \
                      .option("multiLine", True) \
                      .option("quote", "\"") \
                      .option("escape", "\"") \
                      .csv("raw_data/inputs.csv")


providers_df = spark.read.csv("raw_data/providers.csv", header=True, inferSchema=True)
provision_df = spark.read.csv("raw_data/provision.csv", header=True, inferSchema=True)
sequence_df = spark.read.csv("raw_data/sequence.csv", header=True, inferSchema=True)

# Load stages.csv with proper parsing options
stages_df = spark.read.option("header", True) \
                      .option("multiLine", True) \
                      .option("quote", "\"") \
                      .option("escape", "\"") \
                      .csv("raw_data/stages.csv")


# Cleanning

# Find and drop columns with all NULLs
def clean_redundant_cols(df, df_name, redundant_cols=None, null_threshold=0.9):
    total_rows = df.count()

    # Count non-NULL values per column
    non_null_counts = df.agg(*[count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    
    # Identify all-NULL columns (non_null_counts = 0)
    all_null_cols = [c for c, cnt in non_null_counts.items() if cnt == 0]

    # Identify mostly-NULL cols (e.g., > 90% NULL)
    mostly_null_cols = [c for c, cnt in non_null_counts.items() if cnt > 0 and (cnt / total_rows) < (1 - null_threshold)]

    # Check for constant-value (redundant) columns (e.g., provider_type)
    distinct_cnts = df.agg(*[count_distinct(col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
    constant_cols = [c for c, cnt in distinct_cnts.items() if cnt == 1]

    # Drop all redundant and NULL values

    # Drop the all-null columns
    drop_cols = list(set(all_null_cols + mostly_null_cols + (redundant_cols or []) + constant_cols))
    print(f"{df_name}: Dropping columns - All NULL: {all_null_cols}, Mostly NULL: {mostly_null_cols}, Constant: {constant_cols}, Specified: {redundant_cols}")

    if drop_cols:
        df = df.drop(*drop_cols)
    return df 


inputs_df = clean_redundant_cols(inputs_df, "inputs_df")
providers_df = clean_redundant_cols(providers_df, "providers_df")
provision_df = clean_redundant_cols(provision_df, "provision_df")
sequence_df = clean_redundant_cols(sequence_df, "sequence_df")
stages_df = clean_redundant_cols(stages_df, "stages_df")


# provision_df, share_provided from "61%" to 61.0
provision_df = provision_df.withColumn(
    "share_provided",
    regexp_replace(col("share_provided"), "%", "").cast("float")
)

# inputs_df, stages_df, parse market_share_chart_global_market_size_info into market_size(float) and year.
# Parse market size
inputs_df = inputs_df.withColumn(
    "market_size",
    regexp_extract(col("market_share_chart_global_market_size_info"), r"\$([\d.]+)\s*(billion|million)", 1).cast("float") *
    when(regexp_extract(col("market_share_chart_global_market_size_info"), r"\$([\d.]+)\s*(billion|million)", 2) == "billion", 1000).otherwise(1)
).withColumn(
    "year",
    regexp_extract(col("market_share_chart_global_market_size_info"), r"\((\d{4})\)", 1).cast("int")
)

stages_df = stages_df.withColumn(
    "market_size",
    regexp_extract(col("market_share_chart_global_market_size_info"), r"\$([\d.]+)\s*(billion|million)", 1).cast("float") *
    when(regexp_extract(col("market_share_chart_global_market_size_info"), r"\$([\d.]+)\s*(billion|million)", 2) == "billion", 1000).otherwise(1)
).withColumn(
    "year",
    regexp_extract(col("market_share_chart_global_market_size_info"), r"\((\d{4})\)", 1).cast("int")
)


# Clean Text Formatting
# stages_df: replace newlines (\n\n) with spaces in description 
stages_df = stages_df.withColumn(
    "description",
    regexp_replace(col("description"), r"\n+", " ")
)


# Step 2: Remove trailing commas and periods within quotes using UDF
@udf(StringType())
def clean_quoted_text(s):
    import re
    # Remove ‚ first, then trailing commas/periods within quotes
    s = re.sub(r"\u201A", "", s) if s else s 
    return re.sub(r'"([^"]+)[,.]+"', r'"\1"', s) if s else s

inputs_df = inputs_df.withColumn("description", clean_quoted_text(col("description")))

stages_df = stages_df.withColumn("description", clean_quoted_text(col("description")))

# Enhance providers_df: Fill country NULLs where provider_type is "country"
providers_df = providers_df.withColumn(
    "country",
    when(col("provider_type") == "country", col("provider_name")).otherwise(col("country"))
)


# Pre-filter examination: Validate and count outliers
print("\nPre-filter examination of provision_df:")
sums_df_pre = provision_df.groupBy("provided_id").agg(sum_("share_provided").alias("total_share"))
total_ids_pre = sums_df_pre.count()
over_100_pre = sums_df_pre.filter(col("total_share") > 110).count()
null_sums_pre = sums_df_pre.filter(col("total_share").isNull()).count()

print("Validation Summary (pre-filter):")
print(f"Total provided_ids: {total_ids_pre}")
print(f"Outliers with total_share > 110: {over_100_pre} ({(over_100_pre / total_ids_pre) * 100:.2f}%)")
print(f"provided_ids with total_share = NULL: {null_sums_pre} ({(null_sums_pre / total_ids_pre) * 100:.2f}%)")
# Display all outlier rows (> 110)
print("\nOutlier rows with total_share > 110 (pre-filter):")
sums_df_pre.filter(col("total_share") > 110).show(truncate=False)
print("\nRows with total_share = NULL (pre-filter):")
sums_df_pre.filter(col("total_share") > 110).show(truncate=False)
print("\nValidating provision_df sums by provided_id (pre-filter):")
sums_df_pre.show(truncate=False)

# Examine raw data for specific provided_ids with outliers 
outlier_ids = [row["provided_id"] for row in sums_df_pre.filter(col("total_share") > 110).select("provided_id").collect()]
print(f"Outlier provided_ids (pre-filter): {outlier_ids}")
# Use dynamic outlier_ids instead of hardcodedl ist 
print(f"\nRaw data for outlier provided_ids {outlier_ids} (pre-filter):")
provision_df.filter(col("provided_id").isin(outlier_ids)).show(truncate=False)

outlier_df = provision_df.filter(col("provided_id").isin(outlier_ids)) \
                         .join(providers_df.select("provider_id", "provider_type"), "provider_id", "left") \
                         .groupBy("provided_id", "provider_id", "provided_name", "provider_type", "share_provided") \
                         .agg(count("*").alias("row_count")) \
                         .orderBy("provided_id", "provider_id")
outlier_df.show(truncate=False)

# Summarize row counts per provided_id
print("\nRow counts per outlier provided_id (pre-filter):")
outlier_df.groupBy("provided_id").agg(sum_("row_count").alias("total_rows")).show(truncate=False)

# NEW: Filter provision_df to keep only country-level rows
provision_df = provision_df.join(providers_df.select("provider_id", "provider_type"), "provider_id", "left") \
                           .filter(col("provider_type") == "country") \
                           .drop("provider_type")


# Post-filter examination
print("\nPost-filter examination of provision_df:") 
sums_df = provision_df.groupBy("provided_id").agg(sum_("share_provided").alias("total_share"))
# over_100 = sums_df_post.filter(col("total_share") > 110).count()
# Validate sums post-filter
# sums_df = provision_df.groupBy("provided_id").agg(sum_("share_provided").alias("total_share"))
total_ids = sums_df.count()
over_100 = sums_df.filter(col("total_share") > 110).count()
null_sums = sums_df.filter(col("total_share").isNull()).count()

print("Validation Summary (post-filter):")
print(f"Total provided_ids: {total_ids}")
print(f"Outliers with total_share > 110: {over_100} ({(over_100 / total_ids) * 100:.2f}%)")
print(f"provided_ids with total_share = NULL: {null_sums} ({(null_sums / total_ids) * 100:.2f}%)")
print("\nOutlier rows with total_share > 110 (post-filter):")
sums_df.filter(col("total_share") > 110).show(truncate=False)
print("\nValidating provision_df sums by provided_id (post-filter):")
sums_df.show(truncate=False)
print(f"Post filter, > 100 outliers count is {over_100}")

# Display DataFrame name and content 
dfs = [
    ("inputs_df", inputs_df),
    ("providers_df", providers_df),
    ("provision_df", provision_df),
    ("sequence_df", sequence_df),
    ("stages_df", stages_df)
]

for df_name, df in dfs:
    print(f"\n=== {df_name} ===")
    df.show(10, truncate=False)
    print("=" * 50)

# Cleanup
spark.stop()



