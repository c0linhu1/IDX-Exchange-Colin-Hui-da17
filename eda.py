import pandas as pd


sold = pd.read_csv("sold_final.csv", low_memory=False)
listed = pd.read_csv("listed_final.csv", low_memory=False)

# converting to pandas datetimes
sold["CloseDate"] = pd.to_datetime(sold["CloseDate"], errors="coerce")
sold["ListingContractDate"] = pd.to_datetime(sold["ListingContractDate"], errors="coerce")
sold["PurchaseContractDate"] = pd.to_datetime(sold["PurchaseContractDate"], errors="coerce")
listed["ListingContractDate"] = pd.to_datetime(listed["ListingContractDate"], errors="coerce")


# check
print(f"\nShape: {sold.shape}")
print(f"Rows: {sold.shape[0]}, Columns: {sold.shape[1]}")
print(f"\nColumn dtypes:\n{sold.dtypes.value_counts()}")


# null analysis

missing = sold.isnull().sum()
missing_pct = round((missing / len(sold) * 100), 2)

missing_report = pd.DataFrame({
    "missing_count": missing,
    "missing_pct": missing_pct
}).sort_values("missing_pct", ascending=False)


print("\nMissing Value Report (Sold):\n")
print(missing_report[missing_report["missing_count"] > 0].to_string())

high_null = missing_report[missing_report["missing_pct"] > 90]
print(f"\nColumns >90% null ({len(high_null)}):")
if len(high_null) > 0:
    print(high_null.to_string())
else:
    print("None")

# drop vs retain
core_fields = ["ClosePrice", "ListPrice", "LivingArea", "CloseDate",
               "ListingContractDate", "CountyOrParish", "City", "PostalCode",
               "PropertySubType", "BedroomsTotal", "BathroomsTotalInteger",
               "DaysOnMarket", "YearBuilt", "Latitude", "Longitude"]

cols_to_drop = [c for c in high_null.index.tolist() if c not in core_fields]
print(f"\nRecommended to drop (>90% null, non-core): {cols_to_drop}")
print(f"Core fields retained even if partially missing: {[c for c in core_fields if c in high_null.index]}")

# numeric distributions

dist_cols = ["ClosePrice", "ListPrice", "OriginalListPrice", "LivingArea",
             "LotSizeAcres", "BedroomsTotal", "BathroomsTotalInteger",
             "DaysOnMarket", "YearBuilt"]

print("\nNumeric Distribution Summary (Sold)\n")
for col in dist_cols:
    if col in sold.columns:
        s = sold[col].dropna()
        print(f"\n{col}:")
        print(f"count: {len(s)}")
        print(f"mean: {s.mean():.2f}")
        print(f"median: {s.median():.2f}")
        print(f"min: {s.min():.2f}")
        print(f"max: {s.max():.2f}")
        print(f"pct5: {s.quantile(0.05):.2f}")
        print(f"pct25: {s.quantile(0.25):.2f}")
        print(f"pct75: {s.quantile(0.75):.2f}")
        print(f"pct95: {s.quantile(0.95):.2f}")

# suggested interns questions 

# sold 
print('sold intern questions:')

# 1. Property type share should be 100 %
print(f"\nProperty types: {sold['PropertyType'].unique()}")

# 2. Median and average close price
print(f"\nMedian ClosePrice: ${sold['ClosePrice'].median():,.2f}")
print(f"Mean ClosePrice:   ${sold['ClosePrice'].mean():,.2f}")

# 3. Days on market distribution
dom = sold["DaysOnMarket"].dropna()
print(f"\nDaysOnMarket - median: {dom.median():.0f}, mean: {dom.mean():.1f}, max: {dom.max():.0f}")

# 4. Sold above vs below list price
if "ListPrice" in sold.columns:
    above = (sold["ClosePrice"] > sold["ListPrice"]).sum()
    below = (sold["ClosePrice"] < sold["ListPrice"]).sum()
    equal = (sold["ClosePrice"] == sold["ListPrice"]).sum()
    total = above + below + equal
    print(f"\nSold above list: {above} ({above/total*100:.1f}%)")
    print(f"Sold below list: {below} ({below/total*100:.1f}%)")
    print(f"Sold at list: {equal} ({equal/total*100:.1f}%)")

# 5. Date consistency issues
bad_dates = (sold["CloseDate"] < sold["ListingContractDate"]).sum()
print(f"\nClose before listing date: {bad_dates} records")

# 6. Highest median prices by county
print("\nTop 10 counties by median ClosePrice:")
county_median = (sold.groupby("CountyOrParish")["ClosePrice"]
                 .median()
                 .sort_values(ascending=False)
                 .head(10))
print(county_median.to_string())


# doing same for listed

print('listed interns questions:' \
'')
print(f"\nShape: {listed.shape}")

missing_l = listed.isnull().sum()
missing_pct_l = (missing_l / len(listed) * 100).round(2)
missing_report_l = pd.DataFrame({
    "missing_count": missing_l,
    "missing_pct": missing_pct_l
}).sort_values("missing_pct", ascending=False)

print("\nMissing Value Report")
print(missing_report_l[missing_report_l["missing_count"] > 0].to_string())

high_null_l = missing_report_l[missing_report_l["missing_pct"] > 90]
print(f"\nColumns >90% null ({len(high_null_l)}):")
if len(high_null_l) > 0:
    print(high_null_l.to_string())
else:
    print("None")

cols_to_drop_l = [c for c in high_null_l.index.tolist() if c not in core_fields]
print(f"\nRecommended to drop (>90% null, non-core): {cols_to_drop_l}")
print(f"Core fields retained even if partially missing: {[c for c in core_fields if c in high_null_l.index]}")

print("\nNumeric Distribution Summary (Listed)")
list_dist_cols = ["ListPrice", "LivingArea", "LotSizeAcres",
                  "BedroomsTotal", "BathroomsTotalInteger", "DaysOnMarket", "YearBuilt"]
for col in list_dist_cols:
    if col in listed.columns:
        s = listed[col].dropna()
        print(f"\n{col}:")
        print(f"count: {len(s)}")
        print(f"mean: {s.mean():.2f}")
        print(f"median: {s.median():.2f}")
        print(f"min: {s.min():.2f}")
        print(f"max: {s.max():.2f}")
        print(f"pct5: {s.quantile(0.05):.2f}")
        print(f"pct25: {s.quantile(0.25):.2f}")
        print(f"pct75: {s.quantile(0.75):.2f}")
        print(f"pct95: {s.quantile(0.95):.2f}")

