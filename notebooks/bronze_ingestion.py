# notebooks/bronze_ingestion.py
# Ingest all CSV files into Bronze Delta Tables

# Configure path to your uploaded files
storage_path = "/Users/suresh.babu/MDM"
bronze_schema = "workspace.sandbox_bronze"

files = ["Product", "Region", "Reseller", "Salesperson", "SalespersonRegion", "Targets"]

for file_name in files:
    print(f"Ingesting {file_name} into Bronze layer...")
    
    # Read tab-separated file
    df = (spark.read
          .option("header", "true")
          .option("delimiter", "\t")
          .option("inferSchema", "true")
          .csv(f"{storage_path}/{file_name}.csv"))
    
    # Clean up column names by replacing spaces/hyphens with underscores
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.replace(" ", "_").replace("-", "_"))
        
    # Write as a Delta table
    df.write.mode("overwrite").saveAsTable(f"{bronze_schema}.{file_name.lower()}")