import time ,re,os
from datetime import datetime
import dateutil
import pandas as pd
from dateutil.parser import parse


class OperationExecutor:
    
    def __init__(self):
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
    
    def save_tables_to_excel(tables, output_dir="tables", output_file="all_tables.xlsx", separator=True):
        """
        Save tables to Excel.
        If separator=True, saves all tables in one file with multiple sheets.
        If separator=False, saves each table in a separate Excel file.
        """
        if separator:
            full_path = os.path.join(output_dir, output_file)
            with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
                for i, table in enumerate(tables):
                    df = pd.read_html(str(table))[0]
                    sheet_name = f"Table_{i+1}"
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"Added sheet: {sheet_name}")
            # return full_path
        else:
            saved_files = []
            for i, table in enumerate(tables):
                df = pd.read_html(str(table))[0]
                file_path = os.path.join(output_dir, f"table_{i+1}.xlsx")
                df.to_excel(file_path, index=False)
                saved_files.append(file_path)
                print(f"Saved: {file_path}")
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

    
    def runner(self, data, func_name):
        func = getattr(self, func_name, None)  #get function
        if not callable(func): 
            raise ValueError(f"Function '{func_name}' not found in class.")

        p_dict = data.copy()
        records = p_dict.get("records", [])
        for record in records:
            
            print(f"Processing for Bank :{record["bank_name"]}")
            response_data = record.get("response", {})
            if not response_data:
                continue
            post_func_data = {key: func(value) for key, value in response_data.items() if value}
            record["response"] = post_func_data
            
        return p_dict
