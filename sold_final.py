import pandas as pd
import glob as gl


files = sorted(gl.glob("/Users/colin/Downloads/IDX Exchange/raw/CRMLSSold*.csv"))

print(f"Loading {len(files)} sold files...")

# change after run only add new csv file for each month
df = pd.concat([pd.read_csv(f) for f in files], ignore_index = True)

print(f"Shape of df: {df.shape}")

# SAFETY CLEANING
# convert dates
date_cols = ["CloseDate", "ListingContractDate", "PurchaseContractDate"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

# convert numeric columns 
numeric_cols = ["ClosePrice", "ListPrice", "LivingArea", "LotSizeSquareFeet"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Drop duplicates
df = df.drop_duplicates(subset=["ListingKey"], keep="last")



# FEATURE ENGINEERING
# Price per square foot
df["price_per_sqft"] = df["ClosePrice"] / df["LivingArea"]

# Days to close
df["days_to_close"] = (df["CloseDate"] - df["ListingContractDate"]).dt.days

# Month + Year (for Tableau)
df["year"] = df["CloseDate"].dt.year
df["month"] = df["CloseDate"].dt.month

# Property age
df["property_age"] = df["year"] - df["YearBuilt"]

# ---------------------------
# 4. Optional Filtering
# ---------------------------
# Remove extreme outliers (optional but good practice)
df = df[df["ClosePrice"] > 10000]
df = df[df["LivingArea"] > 100]

# ---------------------------
# 5. Save final dataset
# ---------------------------
output_file = "sold_final.csv"
df.to_csv(output_file, index=False)

print(f"Final shape: {df.shape}")
print(f"Saved to {output_file}")