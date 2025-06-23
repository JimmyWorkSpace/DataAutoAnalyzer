import os
import pandas as pd
import json

class DataProcessor:
    """
    A class to process and enrich TransferTime Excel data using FabShelf mapping.
    Responsibilities include data loading, enrichment, column reordering, and providing final DataFrame.
    """

    def __init__(self, file_path: str):
        """
        Initializes the processor with the Excel file path and extracts sheet names.
        :param file_path: Full path to the Excel file
        """
        self.file_path = file_path
        self.sheet_names = pd.ExcelFile(file_path).sheet_names
        self.df_transfer = None
        self.df_shelf = None

    def load_sheets(self):
        """
        Loads the TransferTime sheet (auto-detected by prefix) and loads FabShelf mapping from data/FabShelf.json.
        Raises an error if TransferTime sheet is not found.
        """
        transfer_sheet = next((s for s in self.sheet_names if s.startswith('TransferTime')), None)
        if not transfer_sheet:
            raise ValueError("❌ No sheet starting with 'TransferTime' found.")

        self.df_transfer = pd.read_excel(self.file_path, sheet_name=transfer_sheet)

        # Load FabShelf.json from data/ directory
        root_path = os.path.dirname(__file__)
        fab_shelf_path = os.path.join(root_path, "data", "FabShelf.json")
        if not os.path.exists(fab_shelf_path):
            raise FileNotFoundError(f"❌ FabShelf.json not found at: {fab_shelf_path}")

        with open(fab_shelf_path, 'r', encoding='utf-8') as f:
            shelf_data = json.load(f)

        # Auto-detect JSON structure: dict vs list
        if isinstance(shelf_data, list):
            self.df_shelf = pd.DataFrame(shelf_data)
        else:
            self.df_shelf = pd.DataFrame.from_dict(shelf_data, orient='index').reset_index()
            self.df_shelf.columns = ['SHELF_NAME', 'Area', 'Bay']

    def enrich_data(self):
        """
        Enriches df_transfer with:
        - Area and Bay info for source/destination
        - FAB flow classification type
        - Date and Hour from CREATE TIME
        Also triggers reordering of relevant columns.
        """
        shelf_dict = self.df_shelf.set_index('SHELF_NAME')[['Area', 'Bay']].to_dict(orient='index')

        def get_area_bay(shelf_name):
            info = shelf_dict.get(shelf_name, {})
            return info.get('Bay', None), info.get('Area', None)

        self.df_transfer[['SRC_BAY', 'SOURCE_Area']] = self.df_transfer['COMMAND SOURCE'].apply(
            lambda x: pd.Series(get_area_bay(x))
        )

        self.df_transfer[['DEST_BAY', 'DEST_Area']] = self.df_transfer['COMMAND DESTINATION'].apply(
            lambda x: pd.Series(get_area_bay(x))
        )

        def classify_type(row):
            sa, da = row['SOURCE_Area'], row['DEST_Area']
            return f'FAB{sa}->FAB{da}' if sa in [1, 2] and da in [1, 2] else 'N/A'

        self.df_transfer['Type'] = self.df_transfer.apply(classify_type, axis=1)

        if 'CREATE TIME' in self.df_transfer.columns:
            self.df_transfer['Date'] = self.df_transfer['CREATE TIME'].dt.strftime('%Y-%m-%d')
            self.df_transfer['Hour'] = self.df_transfer['CREATE TIME'].dt.hour
            self._reorder_after_create_time()

        self._reorder_columns()

    def _reorder_after_create_time(self):
        cols = list(self.df_transfer.columns)
        create_time_index = cols.index('CREATE TIME')
        for col in ['Hour', 'Date']:
            if col in cols:
                cols.remove(col)
        cols = cols[:create_time_index + 1] + ['Date', 'Hour'] + cols[create_time_index + 1:]
        self.df_transfer = self.df_transfer[cols]

    def _reorder_columns(self):
        cols = list(self.df_transfer.columns)

        for col in ['SRC_BAY', 'SOURCE_Area']:
            if col in cols:
                cols.remove(col)
        src_index = cols.index('COMMAND SOURCE')
        cols = cols[:src_index + 1] + ['SRC_BAY', 'SOURCE_Area'] + cols[src_index + 1:]

        for col in ['DEST_BAY', 'DEST_Area']:
            if col in cols:
                cols.remove(col)
        dest_index = cols.index('COMMAND DESTINATION')
        cols = cols[:dest_index + 1] + ['DEST_BAY', 'DEST_Area'] + cols[dest_index + 1:]

        if 'Type' in cols:
            cols.remove('Type')
        dest_area_index = cols.index('DEST_Area')
        cols = cols[:dest_area_index + 1] + ['Type'] + cols[dest_area_index + 1:]

        self.df_transfer = self.df_transfer[cols]

    def get_transformed_data(self) -> pd.DataFrame:
        return self.df_transfer
