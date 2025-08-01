import os, re, inspect,sys, ocrmypdf, camelot # type: ignore
import fitz # type: ignore
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# TableParser handles PDF table extraction, cleaning, and formatting.
# It uses Camelot for extracting tables and includes utilities to clean text,
# normalize data, match keywords, and extract meaningful sections from DataFrames.
# Designed to support parsing SID/KIM mutual fund documents.

class TableParser:
    
    def __init__(self):
        self.pipeline = {
            'remove_extra_whitespace': lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x,
            'strip_edges': lambda x: x.strip() if isinstance(x, str) else x,
            'lowercase': lambda x: x.lower() if isinstance(x, str) else x,
            'newline_to_space': lambda x: x.replace('\n', ' ') if isinstance(x, str) else x,
            'str_to_pd_NA': lambda x: pd.NA if isinstance(x, str) and re.match(r'^\s*$', x) else x,
            'normalize_alphanumeric': lambda x: re.sub(r'\s+', ' ', re.sub(r'[^a-zA-Z0-9]+', ' ', x)).strip().lower() if isinstance(x, str) else x,
            'NA_to_str': lambda x: "" if x is pd.NA or pd.isna(x) else x,
            'drop_all_na': lambda df: df.dropna(axis=0, how='all').dropna(axis=1, how='all') if isinstance(df, pd.DataFrame) else df
        }
        
    
    def clean_dataframe(self, df, steps, columns=None):
        """Apply a sequence of cleaning functions to specified columns in a DataFrame.
        Args:   df (pd.DataFrame): The input DataFrame to clean.
                steps (list): List of function names (as keys in self.pipeline) to apply.
                columns (list, optional): List of column names to clean. If None, all columns are used.
        Returns: pd.DataFrame: Cleaned DataFrame."""
        
        cols = columns or df.columns
        for step in steps:
            df[cols] = df[cols].map(self.pipeline[step])
        return df
    
    def clean_series(self, series, steps):
        """Apply a sequence of cleaning functions to a pandas Series.
        Args:   series (pd.Series): The input Series to clean.
                steps (list): List of function names (as keys in self.pipeline) to apply.
        Returns:pd.Series: Cleaned Series."""
        
        for step in steps:
            series = series.apply(self.pipeline[step])
        return series
    
    def extract_tables_from_pdf(self,path, pages, flavor='lattice',stack= True, padding = 1):
        """Extract tables from a PDF file using Camelot and return combined DataFrame.
        Args:   path (str): Path to the PDF file.
                pages (str): Pages to extract (e.g., '1,2' or '1-3').
                flavor (str): Camelot flavor to use ('lattice' or 'stream').
                stack (bool): Whether to stack all tables vertically.
                padding (int): Number of empty rows to add between stacked tables.
        Returns:pd.DataFrame: Combined DataFrame of extracted tables."""
        
        tables = camelot.read_pdf(path, pages=pages, flavor=flavor)
        dfs = [t.df for t in tables]
        return self._concat_padding_vertical(*dfs,padding_rows=padding) if stack else self._concat_padding_horizontal(*dfs,padding_rows=padding)
    
    def get_matching_row_indices(self, df, keywords, thresh):
        """Find row indices in a DataFrame that match a set of keywords in at least 'thresh' cells.
        Args:   df (pd.DataFrame): DataFrame to search.
                keywords (list): List of keyword strings to match.
                thresh (int): Minimum number of matching cells required per row.
        Returns:list: List of matching row indices. Returns [0] if none found."""
        
        pattern = re.compile("|".join(keywords), re.IGNORECASE)
        # print(pattern)
        matched_rows = []
        for idx, row in df.iterrows():
            match_count = 0
            for cell in row:
                cell_text =self._regex_._normalize_alphanumeric(str(cell))
                # print(cell_text)
                if pattern.search(cell_text):  # use .match to anchor to start
                    match_count += 1
                    if match_count >= thresh:
                        matched_rows.append(idx)
                        break
        return matched_rows if matched_rows else [0]
    
    def get_matching_col_indices(self, df, keywords, thresh=1):
        """Find column names where content matches the given keywords at least 'thresh' times.
        Args:   df (pd.DataFrame): DataFrame to search.
                keywords (list): List of keyword strings to match.
                thresh (int): Minimum number of matches required per column.
                match_start_only (bool): (Unused currently) If True, match only at the start of text.
        Returns:list: List of matching column names. Returns [0] if none found."""
    
        pattern = re.compile(rf"({'|'.join(keywords)})", re.IGNORECASE)
        matched_cols = []
        for col in df.columns:
            col_text = self._regex_._normalize_alphanumeric(" ".join(map(str, df[col].fillna("").astype(str))))
            match_count = 0
            for _ in pattern.finditer(col_text):
                match_count += 1
                if match_count >= thresh:
                    matched_cols.append(col)
                    break  # Stop scanning this column further

        return matched_cols if matched_cols else [0]

    def _concat_padding_vertical(self,*dfs, padding_rows=1)->pd.DataFrame:
        result = pd.DataFrame()
        padding = pd.DataFrame([[""] * dfs[0].shape[1]] * padding_rows, columns=dfs[0].columns)
        for i, df in enumerate(dfs):
            result = pd.concat([result, df], ignore_index=True)
            if i < len(dfs) - 1:
                result = pd.concat([result, padding], ignore_index=True)
        return result

    def _concat_padding_horizontal(self,*dfs, padding_cols=1)->pd.DataFrame:
        result = pd.DataFrame()
        num_rows = dfs[0].shape[0]
        padding = pd.DataFrame([[""] * padding_cols] * num_rows)
        for i, df in enumerate(dfs):
            result = pd.concat([result, df], axis=1)
            if i < len(dfs) - 1:
                result = pd.concat([result, padding], axis=1)
        return result
    
    def get_sub_dataframe(self, df, rs=0, re=None, cs=0, ce=None): 
        """Extract a sub-section of the DataFrame with optional row and column slicing.
        Args:
            df (pd.DataFrame): The input DataFrame.
            rs (int): Starting row index (default is 0).
            re (int or None): Ending row index (exclusive). If None, goes till the end.
            cs (int): Starting column index (default is 0).
            ce (int or None): Ending column index (exclusive). If None, goes till the end.
        Returns:pd.DataFrame: A sub-DataFrame with reset index and re-numbered columns."""
        
        max_row, max_col = df.shape

        # Clip indices to valid bounds
        rs = min(rs, max_row - 1) if max_row > 0 else 0
        cs = min(cs, max_col - 1) if max_col > 0 else 0
        re = min(re, max_row) if re is not None else None
        ce = min(ce, max_col) if ce is not None else None

        sub_df = df.iloc[rs:re, cs:ce]
        sub_df.columns = range(sub_df.shape[1])
        sub_df = sub_df.reset_index(drop=True)
        return sub_df
 
    def _drop_na_all(self,dfs,row = True, col = True):
        if row:
            dfs = dfs.dropna(axis=0,how="all")
        if col:
            dfs = dfs.dropna(axis=1,how="all")
        return dfs
    
    def _group_and_collect(self,df, group_col=""):
        final_dict = {}
        data_cols = df.columns.drop(group_col)
        
        for title, group_df in df.groupby(group_col, sort=False):
            norm_title = self._regex_._normalize_key(title)
            values = [
                cell
                for _, row in group_df.iterrows()
                for cell in row[data_cols]
                if isinstance(cell, str) and cell.strip()
            ]
            final_dict[norm_title] = values

        return final_dict