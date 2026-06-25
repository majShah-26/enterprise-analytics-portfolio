import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create the data target directories locally across all 4 analytics folders
os.makedirs("02-energy-analytics/raw_data", exist_ok=True)
os.makedirs("03-finance-analytics/raw_data", exist_ok=True)
os.makedirs("04-construction-analytics/raw_data", exist_ok=True)
os.makedirs("05-refining-analytics/raw_data", exist_ok=True)

# Set random seed for repeatable results across dimensions
np.random.seed(42)
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
months_range = pd.date_range(start="2024-01-01", end="2025-12-01", freq="MS")

# =========================================================================
# MODULE 1: INDUSTRIAL ENERGY & INFRASTRUCTURE DATASET
# =========================================================================
print("Processing: Generating Universal Energy Operations data...")
asset_ids = [f"AST-{i:03d}" for i in range(1, 21)]
asset_types = ["Generation Plant", "Distribution Substation", "Transmission Node", "Processing Hub", "Storage Facility"]
regions = ["Territory Alpha", "Territory Beta", "Territory Gamma", "Territory Delta", "Territory Epsilon"]

dim_assets = pd.DataFrame({
    "AssetID": asset_ids,
    "AssetName": [f"{np.random.choice(asset_types)} - {np.random.choice(regions)}" for _ in asset_ids],
    "AssetType": [np.random.choice(asset_types) for _ in asset_ids],
    "Region": [np.random.choice(regions) for _ in asset_ids],
    "Capacity_Units": np.random.choice([5000, 12000, 25000, 45000, 60000], len(asset_ids))
})

downtime_records = []
failure_types = ["Mechanical Fault", "Electrical Trip", "Scheduled Overhaul", "Integrity Variance", "Control System Alert"]
statuses = ["Resolved", "Resolved", "Pending Final Sign-Off"]

for current_dt in date_range:
    if np.random.rand() > 0.4:  
        affected_assets = np.random.choice(asset_ids, size=np.random.randint(1, 3), replace=False)
        for asset in affected_assets:
            downtime = round(np.random.uniform(0.5, 36.0), 2)
            capacity = dim_assets.loc[dim_assets["AssetID"] == asset, "Capacity_Units"].values[0]
            lost_vol = round((downtime / 24.0) * capacity * np.random.uniform(0.8, 1.0), 2)
            downtime_records.append({
                "AssetID": asset,
                "Date": current_dt.strftime("%Y-%m-%d"),
                "FailureType": np.random.choice(failure_types) if downtime > 4 else "Minor Operational Deviation",
                "DowntimeHours": downtime,
                "LostProductionVolume": lost_vol,
                "MaintenanceStatus": np.random.choice(statuses)
            })

pd.DataFrame(downtime_records).to_csv("02-energy-analytics/raw_data/raw_downtime_events.csv", index=False)
dim_assets.to_csv("02-energy-analytics/raw_data/dim_assets.csv", index=False)

# =========================================================================
# MODULE 2: CORPORATE FINANCE DATASET
# =========================================================================
print("Processing: Generating Corporate Finance & Budget data...")
cost_centers = [f"CC-{i:03d}" for i in range(100, 110)]
departments = ["Strategy & Architecture", "Cloud Engineering", "Enterprise Intelligence", "Automation Core", "Program Management", "Systems Engineering", "Cyber Security", "Human Resources", "Shared Operations", "Corporate Finance"]

dim_cost_centers = pd.DataFrame({
    "CostCenterID": cost_centers,
    "DepartmentName": departments,
    "Division": ["Technology Architecture" if i < 6 else "Enterprise Operations" for i in range(len(cost_centers))],
    "PracticeLead": [f"Practice Lead {i}" for i in range(1, 11)]
})

finance_records = []
for month in months_range:
    for cc in cost_centers:
        base_budget = np.random.choice([150000, 280000, 420000, 650000, 900000])
        variance_factor = np.random.uniform(0.85, 1.15)
        actual_spend = round(base_budget * variance_factor, 2)
        finance_records.append({
            "CostCenterID": cc,
            "MonthStarting": month.strftime("%Y-%m-%d"),
            "AllocatedBudget": base_budget,
            "ActualSpend": actual_spend,
            "OPEX_Component": round(actual_spend * 0.70, 2),
            "CAPEX_Component": round(actual_spend * 0.30, 2)
        })

pd.DataFrame(finance_records).to_csv("03-finance-analytics/raw_data/fact_budget_execution.csv", index=False)
dim_cost_centers.to_csv("03-finance-analytics/raw_data/dim_cost_centers.csv", index=False)

# =========================================================================
# MODULE 3: CAPITAL PROGRAM DELIVERY DATASET (CONSTRUCTION / EVM)
# =========================================================================
print("Processing: Generating Capital Program Delivery data...")
project_ids = [f"PRJ-{i:03d}" for i in range(1, 11)]
project_names = ["Infrastructure Phase A", "Civic Center Structural", "Logistics Grid Expansion", "Utility Ring Main", "Subsurface Earthworks", "Marine Intake Pipeline", "Modular Housing Cluster", "Desalination Interconnect", "Primary Arterial Roads", "HVAC Substation Build"]

dim_projects = pd.DataFrame({
    "ProjectID": project_ids,
    "ProjectName": project_names,
    "ProgramTrack": ["Civic Infrastructure" if i % 2 == 0 else "Industrial Utilities" for i in range(len(project_ids))],
    "RiskClassification": np.random.choice(["Low", "Medium", "High"], len(project_ids))
})

construction_records = []
for current_dt in date_range[::30]: # Monthly reporting snapshots
    for prj in project_ids:
        planned = np.random.choice([1000000, 2500000, 5000000, 7500000])
        # Inject realistic delays (Earned Value falling behind Planned Value, Costs expanding)
        earned = round(planned * np.random.uniform(0.75, 1.02), 2)
        actual = round(earned * np.random.uniform(0.95, 1.20), 2)
        construction_records.append({
            "ProjectID": prj,
            "Date": current_dt.strftime("%Y-%m-%d"),
            "PlannedValue": planned,
            "EarnedValue": earned,
            "ActualCost": actual,
            "ContractorID": f"CNT-{np.random.randint(101, 106)}"
        })

pd.DataFrame(construction_records).to_csv("04-construction-analytics/raw_data/raw_construction_evm.csv", index=False)
dim_projects.to_csv("04-construction-analytics/raw_data/dim_projects.csv", index=False)

# =========================================================================
# MODULE 4: DOWNSTREAM PROCESSING & YIELD DATASET
# =========================================================================
print("Processing: Generating Downstream Processing Yield data...")
plant_ids = [f"PLT-{i:02d}" for i in range(1, 6)]
plant_names = ["Refining Hub Alpha", "Processing complex Beta", "Hydrocarbon Node Gamma", "Petrochemical Matrix Delta", "Separation Facility Epsilon"]

dim_plants = pd.DataFrame({
    "PlantID": plant_ids,
    "PlantName": plant_names,
    "OperationalTier": ["Tier 1 Baseline", "Tier 2 Heavy", "Tier 1 Baseline", "Tier 3 Complex", "Tier 2 Heavy"]
})

refining_records = []
for current_dt in date_range:
    for plt in plant_ids:
        gross_in = round(np.random.uniform(40000, 120000), 2)
        # Yield efficiency between 88% and 97%
        yield_pct = np.random.uniform(0.88, 0.97)
        net_out = round(gross_in * yield_pct, 2)
        refining_records.append({
            "PlantID": plt,
            "Date": current_dt.strftime("%Y-%m-%d"),
            "GrossInput_BBL": gross_in,
            "NetOutput_BBL": net_out,
            "Emission_PPM": round(np.random.uniform(12.0, 48.0) + (1.5 if yield_pct < 0.90 else 0), 3),
            "OperationalEfficiency_Pct": round(yield_pct * 100, 2)
        })

pd.DataFrame(refining_records).to_csv("05-refining-analytics/raw_data/raw_refining_yields.csv", index=False)
dim_plants.to_csv("05-refining-analytics/raw_data/dim_plants.csv", index=False)

print("\nStatus: Success! All 4 universal enterprise datasets successfully generated and filed locally.")
