import pandas as pd

def generate_execute_period_pivot(df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
    """
    Generates a pivot table showing the average EXECUTE PERIOD grouped by Date, Hour, OHT ID, and Type.

    :param df: Input DataFrame with EXECUTE PERIOD column
    :param output_path: Optional path to save the pivot table as Excel
    :return: The pivot DataFrame
    """
    # Validate required columns
    required_cols = {'Date', 'Hour', 'OHT ID', 'Type', 'EXECUTE PERIOD'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns in DataFrame: {required_cols - set(df.columns)}")

    # Create pivot
    pivot_df = pd.pivot_table(
        df,
        index=['Date', 'Hour', 'OHT ID', 'Type'],
        values='EXECUTE PERIOD',
        aggfunc='mean'
    ).reset_index()

    # Optional save
    if output_path:
        pivot_df.to_excel(output_path, index=False)
        print(f"✅ Pivot table saved to: {output_path}")

    return pivot_df
