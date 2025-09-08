import time ,re,os, hmac, hashlib, inspect, dateutil, base64, pdfplumber
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.parser import parse
from io import StringIO, BytesIO
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import PatternFill
from docx import Document
from docx.shared import Pt


class OperationExecutor:
    
    def __init__(self, logger = None):
        
        self.logger = logger
        self.procedures = {
            "ext_date": self.extract_date,
            "sha256": self._generate_hash_sha256,
            "sha1": self._generate_hash_sha1,
            "normalize_df": self._generalize_table_df,
            "original":self._boomerang
        }
        
        self.type_compatibility = {
            "normalize_df": ["table_html"],
            "sha1": ["table_html", "html", "pdf"],
            "sha256": ["table_html", "html", "pdf"],
            "ext_date": ["html"],
            "original": ["pdf", "html", "table_html"]
        }

    #============ PROCEDURES ==============
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
    
    def _boomerang(self,data):
        return data
    
    def _generate_hash_sha256(self,text:str)->str:
        if not isinstance(text,str):
            return f"{inspect.currentframe().f_code.co_name}: input non str"
        return hashlib.sha256(text.encode()).hexdigest()

    def _generate_hash_sha1(self,text:str)->str:
        if not isinstance(text,str):
            return f"{inspect.currentframe().f_code.co_name}: input non str"
        return hashlib.sha1(text.encode()).hexdigest()
    
    def _generalize_table_df(self,html_str)->str:
        MAX_COLUMN=15
        cols = [f"column_{i}" for i in range(1, MAX_COLUMN + 1)]
        dfs = pd.read_html(StringIO(html_str), flavor='html5lib')
        if not dfs:
            return pd.DataFrame(columns=cols)
        
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
        
        norm_df = final_df.to_csv(index=False,header=False, sep='|', lineterminator='\n')
        return norm_df


    
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
  
    #Core Functionality
    def runner(self, data, function_to_execute):
        p_dict = data.copy()
        records = p_dict.get("records", [])

        for record in records:
            # self.logger.info(f"Processing : {record['bank_name']}")
            print(f">>Processing {record['bank_name']}")
            response_data = record.get("scraped_data", [])
            if not response_data:
                continue

            new_scraped_data = []

            for action in response_data:
                if not action.get("data_present"):
                    continue

                response = action.get("response")
                if not response:
                    continue

                for _packet_ in response:
                    check_packet = _packet_.copy()

                    for stage_name, operations in function_to_execute.items():
                        for operation in operations:
                            # Unpack operation with optional expected_type
                            if len(operation) == 4:
                                func_name, source_key, target_key, expected_type = operation
                            else:
                                func_name, source_key, target_key = operation
                                expected_type = None

                            if target_key in check_packet:
                                raise ValueError(
                                    f"`target_key` cannot be similar to any of these keys: {list(check_packet.keys())}"
                                )

                            func = self.procedures.get(func_name)
                            if not func:
                                raise ValueError(f"Function '{func_name}' not found in procedures.")

                            input_value = _packet_.get(source_key)
                            if input_value is None:
                                continue

                            # Apply type check only in primary stage
                            if stage_name == "primary" and expected_type:
                                packet_type = _packet_.get("type", "").lower()
                                if isinstance(expected_type, list):
                                    if packet_type not in [t.lower() for t in expected_type]:
                                        continue
                                elif packet_type != expected_type.lower():
                                    continue

                            _packet_[target_key] = func(input_value)

                    new_scraped_data.append(_packet_)

            record["scraped_data"] = new_scraped_data

        return p_dict
        
    def process_comparison(self, old_json: dict, new_json: dict, key: str = "hash256") -> dict:
    
        def __extract_scraped_items(data, bank_code):
            for record in data.get("records", []):
                if record.get("bank_code") == bank_code:
                    return [entry for entry in record.get("scraped_data", []) if key in entry]
            return []

        def __build_comparison_result(result):
            return {
                "comparison_result": {
                    "new": result["new_packets"],
                    "removed": result["removed_packets"],
                    "unchanged": list(result["unchanged_keys"]),
                    "summary": {
                        "old_total": result["old_total"],
                        "new_total": result["new_total"],
                        "new_count": len(result["new_packets"]),
                        "removed_count": len(result["removed_packets"]),
                        "unchanged_count": len(result["unchanged_keys"])
                    }
                }
            }

        for new_record in new_json.get("records", []):
            bank_code = new_record.get("bank_code")
            old_items = __extract_scraped_items(old_json, bank_code)
            new_items = __extract_scraped_items(new_json, bank_code)

            old_keys = {item[key] for item in old_items}
            new_keys = {item[key] for item in new_items}

            new_only_keys = new_keys - old_keys
            removed_keys = old_keys - new_keys
            unchanged_keys = old_keys & new_keys

            result = {
                "key": key,
                "new_keys": new_only_keys,
                "removed_keys": removed_keys,
                "unchanged_keys": unchanged_keys,
                "new_packets": [item for item in new_items if item[key] in new_only_keys],
                "removed_packets": [item for item in old_items if item[key] in removed_keys],
                "old_total": len(old_items),
                "new_total": len(new_items)
            }

            new_record.update(__build_comparison_result(result))
            new_record.pop("scraped_data", None)

        return new_json

    #Report Generation
    def _write_summary(self, ws, summary, start_row, bank_name="", bank_link=""):
        
        ws.cell(row=start_row, column=1, value="Bank Name")
        ws.cell(row=start_row, column=2, value=bank_name)

        ws.cell(row=start_row + 1, column=1, value="Bank Link")
        ws.cell(row=start_row + 1, column=2, value=bank_link)

        ws.cell(row=start_row + 2, column=1, value="Summary")
        headers = ["Old Total", "New Total", "New Count", "Removed Count", "Unchanged Count", "Change %"]
        values = [
            summary.get("old_total", 0),
            summary.get("new_total", 0),
            summary.get("new_count", 0),
            summary.get("removed_count", 0),
            summary.get("unchanged_count", 0),
            round((summary.get("new_count", 0) + summary.get("removed_count", 0)) / max(summary.get("old_total", 1), 1) * 100, 2)
        ]

        for col, header in enumerate(headers, start=1):
            ws.cell(row=start_row + 3, column=col, value=header)
        for col, val in enumerate(values, start=1):
            ws.cell(row=start_row + 4, column=col, value=val)

        return start_row + 6
    
    def _parse_table(self, entry):
        # print(entry)
        content_type = entry.get("type", "str")  # default to html
        raw_content = entry.get("value","")
        
        try:
            if content_type == "table_html":
                return pd.read_html(StringIO(raw_content))[0]
            
            elif content_type == "html":      
                soup = BeautifulSoup(raw_content, "html.parser")
                return  pd.DataFrame({"text":soup.get_text()})
            
            elif content_type == "pdf":
                pdf_bytes = base64.b64decode(raw_content)
                pdf_file = BytesIO(pdf_bytes)
                all_rows = []
                with pdfplumber.open(pdf_file) as pdf:
                    for page in pdf.pages:
                        table = page.extract_table()
                        if table:
                            all_rows.extend(table)
                return pd.DataFrame(all_rows)
                
            else:
                return pd.DataFrame()  # fallback
        except:
            return pd.DataFrame([["Failed to parse table"]])
             
    def _write_side_by_side_tables(self, ws, new_df, removed_df, start_row, gap=2):
        new_col_start = 1
        removed_col_start = new_df.shape[1] + new_col_start + gap
        comparison_col_start = removed_col_start + removed_df.shape[1] + gap

        SKY_BLUE_FILL = PatternFill(start_color="B3E5FC", end_color="B3E5FC", fill_type="solid")
        GOLD_YELLOW_FILL = PatternFill(start_color="FFE599", end_color="FFE599", fill_type="solid")
        
        max_rows = max(len(new_df), len(removed_df))
        max_cols = max(new_df.shape[1], removed_df.shape[1])

        # Write new_df
        for r_idx, row in enumerate(dataframe_to_rows(new_df, index=False, header=True)):
            for c_idx, val in enumerate(row):
                cell = ws.cell(row=start_row + r_idx, column=new_col_start + c_idx, value=val)
                cell.fill = SKY_BLUE_FILL

        # Write removed_df
        for r_idx, row in enumerate(dataframe_to_rows(removed_df, index=False, header=True)):
            for c_idx, val in enumerate(row):
                cell = ws.cell(row=start_row + r_idx, column=removed_col_start + c_idx, value=val)
                cell.fill = GOLD_YELLOW_FILL

        # Write cell-by-cell comparison formulas
        for r in range(start_row + 1, start_row + max_rows + 1):
            for c in range(max_cols):
                comp_cell = ws.cell(row=r, column=comparison_col_start + c)

                # Create formula like =A2=G2
                new_col_letter = ws.cell(row=1, column=new_col_start + c).column_letter
                removed_col_letter = ws.cell(row=1, column=removed_col_start + c).column_letter
                comp_cell.value = f"={new_col_letter}{r}={removed_col_letter}{r}"
                

        RED_FILL = PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid")  # Soft red

        #Conditional Formatting 
        for c in range(max_cols):
            col_letter = ws.cell(row=1, column=comparison_col_start + c).column_letter
            formula = f'{col_letter}{start_row + 1}=FALSE'  # Start from first data row
            ws.conditional_formatting.add(
                f'{col_letter}{start_row + 1}:{col_letter}{start_row + max_rows}',
                FormulaRule(formula=[formula], fill=RED_FILL)
            )
        return start_row + max_rows + 2
    
    def generate_sorted_excel_report(self, comparison_json, output_path="DepositRate_Comparison_Report.xlsx"):
        # Sort records: prioritize those with new entries
        sorted_records = sorted(
            comparison_json.get("records", []),
            key=lambda r: len(r.get("comparison_result", {}).get("new", [])),
            reverse=True
        )

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for record in sorted_records:
                bank_name = record.get("bank_name")
                bank_code = record.get("bank_code")
                bank_link = record.get("base_url")
                sheet_name = f"{bank_name} ({bank_code})"[:31]

                comparison_result = record.get("comparison_result", {})
                summary = comparison_result.get("summary", {})
                new_entries = comparison_result.get("new", [])
                removed_entries = comparison_result.get("removed", [])

                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                row_cursor = self._write_summary(ws, summary, start_row=1, bank_name=bank_name, bank_link=bank_link)

                max_tables = max(len(new_entries), len(removed_entries))
                for i in range(max_tables):
                    new_df = self._parse_table(new_entries[i]) if i < len(new_entries) else pd.DataFrame()
                    removed_df = self._parse_table(removed_entries[i]) if i < len(removed_entries) else pd.DataFrame()
                    row_cursor = self._write_side_by_side_tables(ws, new_df, removed_df, start_row=row_cursor)

        return output_path
    
    #Doc Generation
    def _parse_entry_for_doc(self,entry):
        content_type = entry.get("type", "str")
        raw_content = entry.get("value", "")
        result = {"type": content_type, "content": None}

        try:
            if content_type == "table_html":
                df = pd.read_html(StringIO(raw_content))[0]
                result["content"] = df

            elif content_type == "html":
                soup = BeautifulSoup(raw_content, "html.parser")
                text = soup.get_text(separator="\n").strip()
                result["content"] = text

            elif content_type == "pdf":
                pdf_bytes = base64.b64decode(raw_content)
                pdf_file = BytesIO(pdf_bytes)
                all_rows = []
                with pdfplumber.open(pdf_file) as pdf:
                    for page in pdf.pages:
                        table = page.extract_table()
                        if table:
                            all_rows.extend(table)
                result["content"] = pd.DataFrame(all_rows) if all_rows else "[No table found in PDF]"

            else:
                result["content"] = "[Unsupported content type]"

        except Exception as e:
            result["content"] = f"[Failed to parse: {e}]"

        return result

    def generate_sorted_doc_report(self,comparison_json, output_path="DepositRate_Comparison_Report.docx"):
        document = Document()

        sorted_records = comparison_json.get("records", [])

        for record in sorted_records:
            bank_name = record.get("bank_name", "")
            bank_code = record.get("bank_code", "")
            bank_link = record.get("base_url", "")
            scraped_data = record.get("scraped_data", [])


            # Bank Header
            document.add_heading(f">> {bank_code} : {bank_name}", level=1)
            document.add_paragraph(f"Website: {bank_link}")
            document.add_paragraph("Summary:")
            document.add_paragraph("")

            # Scraped Data
            for scrape in scraped_data:
                responses = scrape.get("response", [])
                for response_entry in responses:
                    titles = response_entry.get("title", [])
                    parsed = self._parse_entry_for_doc(response_entry)

                    # Add 3 title lines
                    for line in titles:
                        document.add_paragraph(line)

                    # Add content
                    content = parsed["content"]
                    if isinstance(content, pd.DataFrame):
                        table = document.add_table(rows=1, cols=len(content.columns))
                        table.style = 'Table Grid'
                        for i, col_name in enumerate(content.columns):
                            table.cell(0, i).text = str(col_name)
                        for _, row in content.iterrows():
                            row_cells = table.add_row().cells
                            for i, val in enumerate(row):
                                row_cells[i].text = str(val)
                    else:
                        document.add_paragraph(str(content))
                    document.add_paragraph("")
                    document.add_paragraph("")

            document.add_page_break()

        document.save(output_path)
        return output_path
