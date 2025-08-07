import time ,re
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
