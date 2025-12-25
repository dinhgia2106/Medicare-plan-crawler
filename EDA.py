import json

def analyze_json_structure(data, parent_key='', sep='.'):
    fields = []
    
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            fields.append((new_key, type(v).__name__))
            fields.extend(analyze_json_structure(v, new_key, sep=sep))
    elif isinstance(data, list):
        if len(data) > 0:
            fields.extend(analyze_json_structure(data[0], f"{parent_key}[]", sep=sep))
            
    return fields

def print_report(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        all_fields = analyze_json_structure(data)
        
        print(f"--- REPORT STRUCTURE OF JSON FILE: {file_path} ---")
        print(f"{'Field Path':<50} | {'Data Type':<15}")
        print("-" * 70)
        
        for field, dtype in all_fields:
            print(f"{field:<50} | {dtype:<15}")
            
        print("-" * 70)
        print(f"Total number of fields: {len(all_fields)}")
        
    except Exception as e:
        print(f"Error: {e}")

print_report('final_output/medicare_plans.json')