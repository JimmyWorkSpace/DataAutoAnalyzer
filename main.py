import os
import pandas as pd
from datetime import datetime, timedelta
from data_processor import DataProcessor
from pivot_generator import generate_execute_period_pivot

TOTAL_OHT_COUNT = 69

def process_transfer_time_excel(file_path: str) -> pd.DataFrame:
    processor = DataProcessor(file_path)
    processor.load_sheets()
    processor.enrich_data()
    df = processor.get_transformed_data()

    # Convert EXECUTE PERIOD to numeric and drop non-numeric entries
    df['EXECUTE PERIOD'] = pd.to_numeric(df['EXECUTE PERIOD'], errors='coerce')
    before_rows = len(df)
    df = df.dropna(subset=['EXECUTE PERIOD'])
    dropped_rows = before_rows - len(df)
    if dropped_rows > 0:
        print(f"⚠️ Skipped {dropped_rows} rows with non-numeric EXECUTE PERIOD values.")
    return df

def compute_hourly_avg(df: pd.DataFrame) -> pd.DataFrame:
    if 'Hour' not in df.columns or 'EXECUTE PERIOD' not in df.columns:
        raise ValueError("Missing 'Hour' or 'EXECUTE PERIOD' in DataFrame.")
    
    hourly_avg = df.groupby('Hour')['EXECUTE PERIOD'].mean().reset_index()
    hourly_avg.columns = ['Hour', 'Avg_EXECUTE_PERIOD']
    return hourly_avg

def compute_oht_utilization(df: pd.DataFrame, total_oht: int = TOTAL_OHT_COUNT) -> pd.DataFrame:
    used_oht_count = df['OHT ID'].nunique()
    utilization_percent = round((used_oht_count / total_oht) * 100, 2)

    return pd.DataFrame([{
        'Used OHTs': used_oht_count,
        'Total OHTs': total_oht,
        'Utilization (%)': utilization_percent
    }])

if __name__ == "__main__":
    # Locate input file
    data_dir = "data"
    transfer_file = next((f for f in os.listdir(data_dir) if f.startswith("TransferTime")), None)
    if not transfer_file:
        raise FileNotFoundError("❌ No file starting with 'TransferTime' found in the 'data' directory.")
    
    input_path = os.path.join(data_dir, transfer_file)

    # Ensure output folder exists
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # Date tag for sheet naming (use yesterday's date)
    date_tag = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

    # Output file path
    output_file = os.path.join(output_dir, "OHT_Daily_Report.xlsx")
    file_exists = os.path.exists(output_file)

    # Process data
    df_result = process_transfer_time_excel(input_path)
    pivot_df = generate_execute_period_pivot(df_result)
    hourly_df = compute_hourly_avg(df_result)
    utilization_df = compute_oht_utilization(df_result)

    # Write to Excel (append or create)
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a' if file_exists else 'w', if_sheet_exists='replace') as writer:
        df_result.to_excel(writer, sheet_name=f'{date_tag}_Processed', index=False)
        pivot_df.to_excel(writer, sheet_name=f'{date_tag}_PivotSource', index=False)
        hourly_df.to_excel(writer, sheet_name=f'{date_tag}_HourlyADT', index=False)
        utilization_df.to_excel(writer, sheet_name=f'{date_tag}_Utilization', index=False)

    print(f"✅ All results saved to: {output_file}")
