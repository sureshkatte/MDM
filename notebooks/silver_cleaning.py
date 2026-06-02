from pyspark.sql.functions import col, regexp_replace, to_date

# Define namespaces
bronze_schema = "workspace.sandbox_bronze"
silver_schema = "workspace.sandbox_silver"

print(f"Starting Silver Layer processing. Writing to schema: {silver_schema}")

# ==========================================
# 1. CLEAN PRODUCT DIMENSION
# ==========================================
# Objective: Convert Standard_Cost from '$868.63' string to decimal
df_product_raw = spark.table(f"{bronze_schema}.product")

df_product_clean = df_product_raw.withColumn(
    "Standard_Cost", 
    regexp_replace(col("Standard_Cost"), r'[\$,]', '').cast("decimal(18,2)")
)

df_product_clean.write.mode("overwrite").saveAsTable(f"{silver_schema}.dim_product")
print("-> Cleaned 'dim_product' written successfully.")


# ==========================================
# 2. CLEAN TARGETS FACT
# ==========================================
# Objective: Strip symbols from Target, parse long date string 'Friday, December 1, 2017'
df_targets_raw = spark.table(f"{bronze_schema}.targets")

df_targets_clean = df_targets_raw \
    .withColumn("Target_Amount", regexp_replace(col("Target"), r'[\$,]', '').cast("decimal(18,2)")) \
    .withColumn("Target_Month", to_date(col("TargetMonth"), "EEEE, MMMM d, yyyy")) \
    .drop("Target", "TargetMonth")  # Dropping old string columns

df_targets_clean.write.mode("overwrite").saveAsTable(f"{silver_schema}.fact_targets")
print("-> Cleaned 'fact_targets' written successfully.")


# ==========================================
# 3. PASS-THROUGH DIMENSIONS & MAPPINGS
# ==========================================
# Objective: Standardize remaining lookup tables without heavy parsing changes

# Region Table
spark.table(f"{bronze_schema}.region") \
    .write.mode("overwrite").saveAsTable(f"{silver_schema}.dim_region")

# Salesperson Table
spark.table(f"{bronze_schema}.salesperson") \
    .write.mode("overwrite").saveAsTable(f"{silver_schema}.dim_salesperson")

# Salesperson to Region Mapping Table
spark.table(f"{bronze_schema}.salespersonregion") \
    .write.mode("overwrite").saveAsTable(f"{silver_schema}.map_salesperson_region")

# Reseller Table
spark.table(f"{bronze_schema}.reseller") \
    .write.mode("overwrite").saveAsTable(f"{silver_schema}.dim_reseller")

print("-> All structural lookup and mapping dimensions loaded to Silver.")
print("Silver Layer pipeline complete!")