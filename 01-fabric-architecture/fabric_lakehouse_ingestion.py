# -------------------------------------------------------------------------
# NOTEBOOK: Microsoft Fabric Unified Cloud Platform (Multi-Domain Enterprise Pipeline)
# PURPOSE:  Ingest, clean, and write V-Order optimized Delta tables for:
#           1. Asset Operations  2. Corporate Finance  3. Project Delivery  4. Processing Yields
# ARCHITECT: Senior Data & Analytics Leader / Solution Owner
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize the Fabric native Spark cluster session
spark = SparkSession.builder \
    .appName("UnifiedEnterpriseFabricPlatform") \
    .config("spark.sql.parquet.vorder.enabled", "true") \
    .getOrCreate()

# Base OneLake storage paths utilizing standard Fabric shortcut notation
ONELAKE_BRONZE = "abfss://Workspace@://microsoft.com"
ONELAKE_GOLD   = "abfss://Workspace@://microsoft.com"

print("Initializing Multi-Regional Enterprise Processing Engine...")

# --- [MODULE 1: ASSET OPERATIONS INGESTION] ---
print("Ingesting Asset Operations Data...")
downtime_schema = StructType([
    StructField("AssetID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("FailureType", StringType(), True),
    StructField("DowntimeHours", DoubleType(), True),
    StructField("LostProductionVolume", DoubleType(), True),
    StructField("MaintenanceStatus", StringType(), True)
])
df_downtime_bronze = spark.read.format("csv").option("header", "true").schema(downtime_schema).load(f"{ONELAKE_BRONZE}/raw_downtime_events.csv")
df_downtime_silver = df_downtime_bronze.withColumn("EventDate", to_date(col("Date"), "yyyy-MM-dd")).withColumn("DowntimeHours", when(col("DowntimeHours") < 0, 0.0).otherwise(col("DowntimeHours"))).withColumn("LostProductionVolume", when(col("LostProductionVolume") < 0, 0.0).otherwise(col("LostProductionVolume"))).withColumn("IngestedAtTimestamp", current_timestamp()).drop("Date").dropDuplicates(["AssetID", "EventDate", "FailureType"])
fact_downtime_gold = df_downtime_silver.select(col("AssetID").alias("AssetKey"), col("EventDate").alias("DateKey"), col("FailureType"), col("DowntimeHours"), col("LostProductionVolume"), col("MaintenanceStatus"), col("IngestedAtTimestamp"))
fact_downtime_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ONELAKE_GOLD}/FactDowntime")

# --- [MODULE 2: CORPORATE FINANCE INGESTION] ---
print("Ingesting Corporate Finance Data...")
finance_schema = StructType([
    StructField("CostCenterID", StringType(), True),
    StructField("MonthStarting", StringType(), True),
    StructField("AllocatedBudget", DoubleType(), True),
    StructField("ActualSpend", DoubleType(), True),
    StructField("OPEX_Component", DoubleType(), True),
    StructField("CAPEX_Component", DoubleType(), True)
])
df_finance_bronze = spark.read.format("csv").option("header", "true").schema(finance_schema).load(f"{ONELAKE_BRONZE}/fact_budget_execution.csv")
df_finance_silver = df_finance_bronze.withColumn("FiscalMonthStart", to_date(col("MonthStarting"), "yyyy-MM-dd")).withColumn("AllocatedBudget", when(col("AllocatedBudget") < 0, 0.0).otherwise(col("AllocatedBudget"))).withColumn("ActualSpend", when(col("ActualSpend") < 0, 0.0).otherwise(col("ActualSpend"))).withColumn("IngestedAtTimestamp", current_timestamp()).drop("MonthStarting").dropDuplicates(["CostCenterID", "FiscalMonthStart"])
fact_finance_gold = df_finance_silver.select(col("CostCenterID").alias("CostCenterKey"), col("FiscalMonthStart").alias("DateKey"), col("AllocatedBudget"), col("ActualSpend"), col("OPEX_Component"), col("CAPEX_Component"), col("IngestedAtTimestamp"))
fact_finance_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ONELAKE_GOLD}/FactFinance")

# --- [MODULE 3: PROJECT DELIVERY INGESTION] ---
print("Ingesting Project Delivery EVM Data...")
construction_schema = StructType([
    StructField("ProjectID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("PlannedValue", DoubleType(), True),
    StructField("EarnedValue", DoubleType(), True),
    StructField("ActualCost", DoubleType(), True),
    StructField("ContractorID", StringType(), True)
])
df_const_bronze = spark.read.format("csv").option("header", "true").schema(construction_schema).load(f"{ONELAKE_BRONZE}/raw_construction_evm.csv")
df_const_silver = df_const_bronze.withColumn("ReportingDate", to_date(col("Date"), "yyyy-MM-dd")).withColumn("IngestedAt", current_timestamp()).drop("Date").dropDuplicates(["ProjectID", "ReportingDate"])
df_const_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ONELAKE_GOLD}/FactProjectEVM")

# --- [MODULE 4: PROCESSING YIELD INGESTION] ---
print("Ingesting Processing Downstream Yield Data...")
refining_schema = StructType([
    StructField("PlantID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("GrossInput_BBL", DoubleType(), True),
    StructField("NetOutput_BBL", DoubleType(), True),
    StructField("Emission_PPM", DoubleType(), True),
    StructField("OperationalEfficiency_Pct", DoubleType(), True)
])
df_refine_bronze = spark.read.format("csv").option("header", "true").schema(refining_schema).load(f"{ONELAKE_BRONZE}/raw_refining_yields.csv")
df_refine_silver = df_refine_bronze.withColumn("ProcessingDate", to_date(col("Date"), "yyyy-MM-dd")).withColumn("IngestedAt", current_timestamp()).drop("Date").dropDuplicates(["PlantID", "ProcessingDate"])
df_refine_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ONELAKE_GOLD}/FactProcessingYields")

print("Pipeline complete: All multi-sector cloud database entities successfully synchronized and optimized.")
