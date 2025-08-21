import time ,re,os, hmac, hashlib, inspect
from datetime import datetime
import dateutil
import pandas as pd
from dateutil.parser import parse
from io import StringIO
from bs4 import BeautifulSoup

class OperationExecutor:
    
    def __init__(self):
        
        self.procedures = {
            "ext_date": self.extract_date,
            "sha256": self._generate_hash_sha256,
            "sha1": self._generate_hash_sha1,
            "normalize_df": self._generalize_table_df,
            "original":self.boomerang
        }
        
        pass
    
    @staticmethod
    def extract_date(text: str) -> str:
        if not isinstance(text, str):
            print("Invalid data type. Expected string.")
            return ""
        
        output_format = "%Y%m%d"
        date_patterns = [r"(\d{2}[.\-/]+\d{2}[.\-/]+\d{4}",
                        r"\d{1,2}\s*(?:th|st|rd|nd)\s*[A-Za-z]+\s*\d{4}",
                        r"\d{2}[\.\-\/]+[A-Za-z]+[\.\-\/]+\d{4}",
                        r"\d{2}\s*[A-Za-z]+\s*\d{4})"]
        matches = re.findall(r"|".join(date_patterns), text, re.IGNORECASE)
        
        if matches:
            date_str = " ".join(matches)
            try:
                dt_object = parse(date_str, fuzzy=True)
                return dt_object.strftime(output_format)
            except dateutil.parser._parser.ParserError as e:
                print(f"[ERROR]: {e}")
                return date_str
        return text
    
    def _clean_html_thead(self,html_text):
        html_text = re.sub(r"\<thead",r"\<tbody",html_text, re.IGNORECASE)
        html_text = re.sub(r"\</thead",r"\</tbody",html_text, re.IGNORECASE)
        return html_text

    def boomerang(self,data):
        return data
    
    def _generate_hash_sha256(self,text:str)->str:
        if not isinstance(text,str):
            return f"{inspect.currentframe().f_code.co_name}: input non str"
        return hashlib.sha256(text.encode()).hexdigest()

    def _generate_hash_sha1(self,text:str)->str:
        if not isinstance(text,str):
            return f"{inspect.currentframe().f_code.co_name}: input non str"
        return hashlib.sha1(text.encode()).hexdigest()
    
    def _generalize_table_df(self,html_string)->str:
        MAX_COLUMN=15
        cols = [f"column_{i}" for i in range(1, MAX_COLUMN + 1)]
        dfs = pd.read_html(StringIO(html_string), flavor='html5lib')

        all_dfs = []
        for df in dfs:
            if df.empty:
                continue

            df = df.iloc[:, :MAX_COLUMN].copy()  # truncate if too many columns
            df.columns = cols[:df.shape[1]]      # rename existing columns
            for i in range(df.shape[1], MAX_COLUMN):
                df[cols[i]] = ""                 # fill missing columns with empty strings

            df = df.reindex(columns=cols)        # ensure consistent column order
            all_dfs.append(df)

        final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=cols)
        
        normalized_df = final_df.to_csv(index=False,header=False, sep='|', lineterminator='\n')
        return self._generate_hash_sha256(normalized_df)


    
    @staticmethod
    def save_tables_to_excel(tables, output_dir="tables", output_file="all_tables.xlsx", consolidate_save=True):
        """
        Save tables to Excel.
        If separator=True, saves all tables in one file with multiple sheets.
        If separator=False, saves each table in a separate Excel file.
        """
        if consolidate_save:
            full_path = os.path.join(output_dir, output_file)
            with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
                for i, table in enumerate(tables):
                    df = pd.read_html(str(table))[0]
                    sheet_name = f"Table_{i+1}"
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    # print(f"Added sheet: {sheet_name}")
            # return full_path
        else:
            saved_files = []
            for i, table in enumerate(tables):
                df = pd.read_html(str(table))[0]
                file_path = os.path.join(output_dir, f"table_{i+1}.xlsx")
                df.to_excel(file_path, index=False)
                saved_files.append(file_path)
                # print(f"Saved: {file_path}")
            # return saved_files

    @staticmethod
    def save_tables_html(tables,output_dir="output_html",output_file="combined.html",separator=None,wrap_html=True):
        
        if isinstance(tables,str):tables = [tables]
        if separator is None:
            # Save each table separately
            for i, table in enumerate(tables):
                filename = f"content_{i+1}.html"
                path = os.path.join(output_dir, filename)
                with open(path, "w", encoding="utf-8") as f:
                    if wrap_html:
                        f.write("<html><body>\n")
                    f.write(str(table) + "\n")
                    if wrap_html:
                        f.write("</body></html>")
        else:
            # Save all tables in one file with separator
            path = os.path.join(output_dir, output_file)
            with open(path, "w", encoding="utf-8") as f:
                if wrap_html:
                    f.write("<html><body>\n")
                for i, table in enumerate(tables):
                    f.write(str(table) + "\n")
                    if i < len(tables) - 1:
                        f.write(separator + "\n")
                if wrap_html:
                    f.write("</body></html>")

    def runner(self, data, function_to_execute):
        
        #check if all functions exist
        for key, function_name in function_to_execute.items():
            func = getattr(self, function_name, None)
            if not callable(func):
                raise ValueError(f"Function '{function_name}' not found in class.")
            function_to_execute[key] = func
        
        function_to_execute["original"] = getattr(self, "boomerang", None)
        
        #copy + perform tasks
        p_dict = data.copy()
        records = p_dict.get("records", [])
        for record in records:
            print(f"\nProcessing for Bank :{record["bank_name"]}\n==============================\n")
            response_data = record.get("scraped_data", [])
            if not response_data:
                continue
            
            for action in response_data:
                if not action.get("data_present"):
                    continue
                
                #execute functions on values
                response = action.get("response")
                if not response or not isinstance(response, dict):
                    continue

                processed_data = []
                for key, value in response.items():
                    processed_item = {
                        k: f(value) for k, f in function_to_execute.items()
                    }
                    processed_data.append(processed_item)

                action["response"] = processed_data
            
        return p_dict
