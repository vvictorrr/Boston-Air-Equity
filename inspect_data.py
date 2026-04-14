"""
Dataset Inspector — paste the output into Claude for full context.
Usage: python inspect_data.py <path_to_csv>
"""

import sys
import pandas as pd


def inspect(fp):
    if fp.endswith((".xls", ".xlsx")):
        df = pd.read_excel(fp)
    elif fp.endswith(".csv"):
        df = pd.read_csv(fp)
    else:
        raise ValueError(f"Unsupported file type: {fp}")

    print("=" * 60)
    print(f"FILE: {fp}")
    print(f"SHAPE: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"MEMORY: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    print("=" * 60)

    # column types and nulls
    print("\nCOLUMNS:")
    for col in df.columns:
        nuniq = df[col].nunique()
        nulls = df[col].isna().sum()
        print(f"  {col:<25} {str(df[col].dtype):<12} "
              f"{nuniq:>6} unique   {nulls:>4} nulls")

    # unique values for low-cardinality columns (likely categorical)
    print("\nCATEGORICAL VALUES:")
    for col in df.columns:
        if df[col].nunique() <= 20:
            vals = df[col].dropna().unique().tolist()
            print(f"  {col}: {vals}")

    # numeric summary
    numerics = df.select_dtypes(include="number")
    if len(numerics.columns) > 0:
        print("\nNUMERIC SUMMARY:")
        print(numerics.describe().round(4).to_string())

    # first 5 rows
    print("\nFIRST 5 ROWS:")
    print(df.head().to_string())

    # last 3 rows (catch trailing junk)
    print("\nLAST 3 ROWS:")
    print(df.tail(3).to_string())


if __name__ == "__main__":
    print("census_tracts.csv:")
    inspect("census_tracts.csv")
    print("openaq_locations.csv:")
    inspect("openaq_locations.csv")
    print("openaq_measurements.csv:")
    inspect("openaq_measurements.csv")
    print("flight.xls:")
    inspect("flight.xls")