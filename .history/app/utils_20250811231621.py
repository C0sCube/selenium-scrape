import os, re, json, json5, string, shutil, inspect, camelot
from datetime import datetime
import pandas as pd #type:ignore
from typing import List


class Helper:
    
    def __init__(self):
        pass
    #PARSING UTILS
    def get_xlsx_in_folder(self,path:str, expected_file_name ="table_data.xlsx" ) -> dict:
        df = pd.DataFrame()
        for root,_,files in os.walk(path):
            for file_name in files:
                if file_name.endswith(".xlsx") and file_name.lower() == expected_file_name:
                    self.logger.info(f"Excel sheet containing table data found.")
                    full_path = os.path.join(root, file_name)
                    df = pd.read_excel(full_path)
                    return df
        # self.logger.warning(f"{expected_file_name} not found !! Returning empty df.")
        return df


    @staticmethod
    def copy_pdfs_to_folder(dest_folder: str, data):
        os.makedirs(dest_folder, exist_ok=True)

        if isinstance(data, dict):
            file_paths = list(data.values())
        elif isinstance(data, list):
            file_paths = data
        else:
            raise ValueError("Data must be a list of paths or a dict with path values")

        for path in file_paths:
            if not os.path.isfile(path):
                continue
            try:
                file_name = os.path.basename(path)
                dest_path = os.path.join(dest_folder, file_name)
                shutil.copy2(path, dest_path)
            except Exception as e:
                # logger = get_logger()
                # logger.error(f"Failed to copy '{path}' → {dest_folder}: {e}")
                pass
    
    @staticmethod
    def delete_files_and_empty_folder(file_path: str) -> bool:
        try:
            # print(file_path)
            if os.path.exists(file_path):
                os.remove(file_path)
                
                parent_dir = os.path.dirname(file_path)
                print("Remaining:", os.listdir(parent_dir))

                if os.path.isdir(parent_dir) and not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
                return True
            return False
        except Exception as e:
            # logger = get_logger()
            # logger.exception(f"delete_file_and_empty_folder -> {e}")
            return False
        
    @staticmethod
    def delete_amc_pdf(data):
        try:
            for k, path in data.items():
                Helper.delete_files_and_empty_folder(path)
        except Exception as e:
            # logger = get_logger()
            # logger.exception(f"delete_amc_pdf: {e}")
            return
        return
    
    #JSON UN/LOAD 
    @staticmethod
    def save_json(data: dict,path: str, indent: int = 2,typ = "json"):
        with open(path, "w", encoding="utf-8") as f:
            json5.dump(data, f, indent=indent) if typ == "json5" else json.dump(data, f, indent=indent)

    @staticmethod
    def load_json(path: str,typ = "json"):
        with open(path, "r", encoding="utf-8") as f:
            return json5.load(f) if typ == "json5" else json.load(f)
    
    #WRITE TEXT
    @staticmethod
    def save_text(data,path:str,mode = 'w'):
        if not data:
            raise ValueError("Empty data cannot be saved.")
        if mode not in ('w', 'a'):
            raise ValueError(f"Invalid mode '{mode}'. Use 'w' or 'a'.")
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode, encoding='utf-8') as f:
            if isinstance(data, dict):
                # Write K-V Pair
                for k, v in data.items():
                    f.write(f"{k}:{v}\n")
            elif isinstance(data, list):
                # Write New Line
                for k in data:
                    f.write(f"{k}\n")
            elif isinstance(data, str):
                # Write Single String
                f.write(data)
            else:
                raise ValueError(f"Invalid data type: {type(data)}")
        
    @staticmethod        
    def create_dirs(root_path: str, dirs: List[str]) -> List[str]:
        created_paths = []
        for dir_name in dirs:
            full_path = os.path.join(root_path, dir_name)
            os.makedirs(full_path, exist_ok=True)
            created_paths.append(full_path)
        return created_paths

    @staticmethod
    def get_timestamp(sep=":"):
        now = datetime.now()
        return now.strftime(f"%H{sep}%M{sep}%S")
    
    #normal case
    @staticmethod
    def is_numeric(text):
        return bool(re.fullmatch(r'[+-]?(\d+(\.\d*)?|\.\d+)', text))

    @staticmethod
    def is_alphanumeric(text):
        return bool(re.fullmatch(r'[A-Za-z0-9]+', text))

    @staticmethod
    def is_alpha(text):
        return bool(re.fullmatch(r'[A-Za-z]+', text))
        
    @staticmethod
    def _remove_non_word_space_chars(text: str) -> str:
        if not isinstance(text, str):
            return text
        text = re.sub("[^\w\s]", "", text).strip()
        return text
    
    @staticmethod
    def _normalize_whitespace(text: str) -> str:
        if not isinstance(text, str):
            return text
        return re.sub(r"\s+", " ", text).strip()
    
    @staticmethod
    def _normalize_date(text: str) -> str:
        if not isinstance(text, str):
            return text
        text = re.sub(r"[^A-Za-z0-9\s\.\/\,\-\\]+", " ", text).strip()
        return Helper._normalize_whitespace(text)
    
    @staticmethod
    def _normalize_alphanumeric(text: str) -> str:
        if not isinstance(text, str):
            return text
        text = re.sub(r"[^a-zA-Z0-9]+", " ", str(text))
        return re.sub(r"\s+", " ", text).strip().lower()
    
    @staticmethod
    def _normalize_alpha(text: str) -> str:
        if not isinstance(text, str):
            return text
        text = re.sub(r"[^a-zA-Z]+", " ", str(text))
        return re.sub(r"\s+", " ", text).strip().lower()

    @staticmethod
    def _normalize_numeric(text: str) -> str:
        if not isinstance(text, str):
            return text
        text = re.sub(r"[^0-9\.]+", " ", str(text))
        return re.sub(r"\s+", " ", text).strip().lower()
    
    @staticmethod
    def snake_case(text: str) -> str:
        text = re.sub(r'([A-Z]+)', r' \1', text).strip()
        return re.sub(r'[_\s-]+', '_', text).lower()

    @staticmethod
    def camel_case(text: str) -> str:
        text = re.sub(r'[_\s-]+', ' ', text)
        text = text.title()
        return text[0].lower() + text[1:].replace(' ', '')
     
     
    #file read/write   
    @staticmethod
    def read_file(filepath: str) -> str:
        with open(filepath, 'r') as f:
            return f.read()

    @staticmethod
    def write_file(filepath: str, content: str):
        with open(filepath, 'w') as f:
            f.write(content)

    @staticmethod
    def get_file_extension(filename: str) -> str:
        return os.path.splitext(filename)[1]

    #list chunk
    @staticmethod
    def chunk_list(data: list, size: int):
        return [data[i:i + size] for i in range(0, len(data), size)]

    @staticmethod
    def flatten_list(list_of_lists: list):
        return [item for sublist in list_of_lists for item in sublist]
    
    @staticmethod
    def remove_duplicates(data:list):
        return list(dict.fromkeys(data))
    
    #PDF CRUD
    # @staticmethod
    # def get_pdf_text(path:str):
    
    #     doc = fitz.open(path)
    #     text_data = {}
    #     for pgn in range(doc.page_count):
    #         page = doc[pgn]
    #         text = page.get_text("text")
    #         text = text.encode('utf-8', 'ignore').decode('utf-8')
    #         data = text.split('\n')
    #         text_data [pgn] = data
    #     return text_data
    
    # @staticmethod
    # def get_clipped_data(input:str, bboxes:list[set]):
    
    #     document = fitz.open(input)
    #     final_list = []
        
    #     for pgn in range(document.page_count):
    #         page = document[pgn]

    #         blocks = []
    #         for bbox in bboxes:
    #             blocks.extend(page.get_text('dict', clip = bbox)['blocks']) #get all blocks
            
    #         filtered_blocks = [block for block in blocks if block['type']== 0 and 'lines' in block]
    #         sorted_blocks = sorted(filtered_blocks, key= lambda x: (x['bbox'][1], x['bbox'][0]))
            
    #         final_list.append({
    #         "pgn": pgn,
    #         "block": sorted_blocks
    #         })
            
            
    #     document.close()
    #     return final_list
    
    # @staticmethod
    # def get_all_pdf_data(path:str):
    
    #     doc = fitz.open(path)
    #     count = doc.page_count
    #     all_blocks = list()

    #     for pgn in range(count):
    #         page = doc[pgn]
            
    #         blocks = page.get_text('dict')['blocks']
    #         images = page.get_images()
    #         filtered_blocks = [block for block in blocks if block['type']== 0]
    #         sorted_blocks = sorted(filtered_blocks, key=lambda x: x['bbox'][1])
    #         all_blocks.append({
    #             "pgn":pgn,
    #             "blocks":sorted_blocks,
    #             "images": images
    #         })
            
    #         #draw lines
            
    #         lines = fitz.Rect()
            
    #     doc.close()
        
    #     return all_blocks
    
    
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
                cell_text = Helper._normalize_alphanumeric(str(cell))
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
            col_text = Helper._normalize_alphanumeric(" ".join(map(str, df[col].fillna("").astype(str))))
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
            norm_title = Helper._normalize_alphanumeric(title)
            values = [
                cell
                for _, row in group_df.iterrows()
                for cell in row[data_cols]
                if isinstance(cell, str) and cell.strip()
            ]
            final_dict[norm_title] = values

        return final_dict