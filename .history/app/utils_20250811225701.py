import os, re, json, json5, string, shutil, inspect
from datetime import datetime
import pandas as pd #type:ignore
from typing import List


class Helper:
    
    def __init__(self):
        pass
    #PARSING UTILS
    def get_xlsx_in_folder(self,path:str) -> dict:
        df = pd.DataFrame()
        for root,_,files in os.walk(path):
            for file_name in files:
                if file_name.endswith(".xlsx") and file_name.lower() == "table_data.xlsx":
                    self.logger.info(f"Excel sheet containing table data found.")
                    full_path = os.path.join(root, file_name)
                    df = pd.read_excel(full_path)
                    return df
        self.logger.warning(f" 'table_data.xlsx' not found !! Returning empty df.")
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
    def save_text(data,path:str):
        if not data:
            print("Empty Data")
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'a', encoding='utf-8') as f:
            if isinstance(data,dict):
                f.writelines(f"{k}:{v}\n" for k,v in data.items())
            elif isinstance(data,list):
                f.writelines(f"{k}\n" for k in data)
            elif isinstance(data,str):
                f.writelines(data)
            else: print("Invalid type")
     
    @staticmethod        
    def create_dirs(root_path: str, dirs: List[str]) -> List[str]:
        created_paths = []
        for dir_name in dirs:
            full_path = os.path.join(root_path, dir_name)
            os.makedirs(full_path, exist_ok=True)
            created_paths.append(full_path)
        return created_paths

    @staticmethod
    def get_timestamp(mode="json"):

        now = datetime.now()
        
        if mode == "json":
            return now.strftime("%H:%M:%S")
        elif mode == "filename":
            return now.strftime("%H-%M-%S")
        else:
            raise ValueError("Invalid mode. Use 'json' or 'filename'.")
    
    #match type
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
        return MyStaticMethods._normalize_whitespace(text)
    
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
    def _sanitize_fund(fund: str, fund_name: str, escape_pattern, main_scheme_names):
        fund = re.sub(escape_pattern, '', fund)
        fund = MyStaticMethods._normalize_whitespace(fund)
        for key, regex in main_scheme_names[fund_name].items():
            if re.findall(regex, fund, re.IGNORECASE):
                fund = key
                break
        return fund
    
    @staticmethod
    def _to_rgb_tuple(color_int):
        c = color_int & 0xFFFFFF
        r = (c >> 16) & 0xFF
        g = (c >> 8) & 0xFF
        b = c & 0xFF
        return (r/255.0, g/255.0, b/255.0)
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
    
    
