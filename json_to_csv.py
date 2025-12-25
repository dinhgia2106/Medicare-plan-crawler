#!/usr/bin/env python3
"""
Script để chuyển đổi medicare_plans.json sang CSV
Mỗi plan sẽ là một row trong CSV
Flatten tất cả nested fields đến level nhỏ nhất
"""

import json
import csv
import os
from typing import Any, Dict, List

INPUT_FILE = "final_output/medicare_plans.json"
OUTPUT_FILE = "final_output/medicare_plans.csv"


def flatten_value(value: Any, prefix: str = '') -> Dict[str, Any]:
    """
    Recursively flatten any value to atomic fields.
    Returns a dict with flattened keys.
    """
    result = {}
    
    if value is None:
        result[prefix] = None
    elif isinstance(value, dict):
        for k, v in value.items():
            new_key = f"{prefix}_{k}" if prefix else k
            result.update(flatten_value(v, new_key))
    elif isinstance(value, list):
        for i, item in enumerate(value):
            new_key = f"{prefix}_{i}"
            result.update(flatten_value(item, new_key))
    else:
        # Atomic value (str, int, float, bool)
        # Clean up newlines for CSV
        if isinstance(value, str):
            value = value.replace('\n', ' | ')
        result[prefix] = value
    
    return result


def process_plan(zipcode_data: Dict, plan: Dict) -> Dict:
    """
    Process a single plan and return a fully flattened row.
    """
    row = {}
    
    # Zipcode level data
    row['zipcode_index'] = zipcode_data.get('index')
    row['zipcode'] = zipcode_data.get('zipcode')
    row['state'] = zipcode_data.get('state')
    row['city'] = zipcode_data.get('city')
    row['zipcode_status'] = zipcode_data.get('status')
    row['zipcode_totalPlans'] = zipcode_data.get('totalPlans')
    row['zipcode_plansWithDetails'] = zipcode_data.get('plansWithDetails')
    row['zipcode_error'] = zipcode_data.get('error')
    
    # Plan level data (excluding 'details' which will be flattened separately)
    row['plan_status'] = plan.get('status')
    row['plan_planId'] = plan.get('planId')
    row['plan_planName'] = plan.get('planName')
    row['plan_planType'] = plan.get('planType')
    row['plan_monthlyPremium'] = plan.get('monthlyPremium')
    row['plan_estimatedAnnualCost'] = plan.get('estimatedAnnualCost')
    row['plan_starRating'] = plan.get('starRating')
    row['plan_detailsUrl'] = plan.get('detailsUrl')
    row['plan_error'] = plan.get('error')
    row['plan_scrapedAt'] = plan.get('scrapedAt')
    
    # Flatten all details recursively
    details = plan.get('details', {})
    if details:
        flattened_details = flatten_value(details, 'details')
        row.update(flattened_details)
    
    return row


def main():
    print(f"Loading {INPUT_FILE}...")
    
    # Read JSON file
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Loaded {len(data)} zipcode entries")
    
    # Collect all rows and all possible columns
    all_rows = []
    all_columns = set()
    
    # Process each zipcode entry
    total_plans = 0
    for idx, zipcode_data in enumerate(data):
        plans = zipcode_data.get('plans', [])
        for plan in plans:
            row = process_plan(zipcode_data, plan)
            all_rows.append(row)
            all_columns.update(row.keys())
            total_plans += 1
        
        if (idx + 1) % 100 == 0:
            print(f"Processed {idx + 1} zipcodes, {total_plans} plans so far...")
    
    print(f"\nTotal plans processed: {total_plans}")
    print(f"Total columns: {len(all_columns)}")
    
    # Sort columns for consistent order
    # Priority order for important columns first
    priority_cols = [
        'zipcode_index', 'zipcode', 'state', 'city', 'zipcode_status',
        'zipcode_totalPlans', 'zipcode_plansWithDetails', 'zipcode_error',
        'plan_status', 'plan_planId', 'plan_planName', 'plan_planType', 
        'plan_monthlyPremium', 'plan_estimatedAnnualCost', 'plan_starRating', 
        'plan_detailsUrl', 'plan_error', 'plan_scrapedAt',
    ]
    
    # Sort remaining columns
    def column_sort_key(col):
        """Sort key to group related columns together."""
        parts = col.split('_')
        # Try to extract numeric index for proper sorting (0, 1, 2... instead of 0, 1, 10, 11...)
        result = []
        for part in parts:
            try:
                result.append((0, int(part)))  # Numeric parts
            except ValueError:
                result.append((1, part))  # String parts
        return result
    
    remaining_cols = sorted([c for c in all_columns if c not in priority_cols], key=column_sort_key)
    columns = [c for c in priority_cols if c in all_columns] + remaining_cols
    
    # Write CSV
    print(f"\nWriting to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        
        for i, row in enumerate(all_rows):
            # Clean up None values
            cleaned_row = {k: ('' if v is None else v) for k, v in row.items()}
            writer.writerow(cleaned_row)
            
            if (i + 1) % 10000 == 0:
                print(f"Written {i + 1} rows...")
    
    print(f"\n✅ Done! CSV saved to {OUTPUT_FILE}")
    print(f"   Total rows: {len(all_rows)}")
    print(f"   Total columns: {len(columns)}")
    
    # Save column names to a separate file for reference
    columns_file = "final_output/medicare_plans_columns.txt"
    with open(columns_file, 'w', encoding='utf-8') as f:
        f.write(f"Total columns: {len(columns)}\n")
        f.write("=" * 60 + "\n\n")
        for i, col in enumerate(columns, 1):
            f.write(f"{i:4}. {col}\n")
    
    print(f"   Column names saved to: {columns_file}")


if __name__ == "__main__":
    main()
