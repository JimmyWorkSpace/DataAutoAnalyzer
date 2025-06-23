import pandas as pd
import json

# Correct path and file name
excel_path = r'C:\Users\Jimmy\Project\DataAutoAnalyzer\OriginalData.xlsx'
json_path = r'C:\Users\Jimmy\Project\DataAutoAnalyzer\FabShelf.json'

# Read the 'FabShelf' sheet
df = pd.read_excel(excel_path, sheet_name='FabShelf')

# Convert to JSON-friendly format
data = df.to_dict(orient='records')

# Save as JSON
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"✅ Saved {len(data)} records to: {json_path}")
