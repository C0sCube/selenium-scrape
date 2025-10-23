import pandas as pd
import time ,re,os, hashlib, inspect, tempfile
import dateutil, base64, pdfplumber, ocrmypdf
from bs4 import BeautifulSoup
from dateutil.parser import parse
from io import StringIO, BytesIO

from openpyxl.utils.dataframe import dataframe_to_rows  
from openpyxl.styles import PatternFill, Border, Side, Alignment
from openpyxl.formatting.rule import CellIsRule, FormulaRule

from app.logger import get_global_logger

class OperationExecutorLatest:
 
    cache_doc_name = "" 
    def __init__(self, ):
        self.logger = get_global_logger()
        self.procedures = {
            # "ext_date": self.extract_date,
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
        
        self.MAX_COLUMN_DF = 15
        
        #color fills
        self.SKY_BLUE_FILL = PatternFill(start_color="B3E5FC", end_color="B3E5FC", fill_type="solid")
        self.GREY_FILL = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
        self.RED_FILL = PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid")
        
        # RED_FILL = PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid")
        self.YELLOW_FILL = PatternFill(start_color="FFFF99", end_color="FFFF99", fill_type="solid")
        self.GREEN_FILL = PatternFill(start_color="CCFFCC", end_color="CCFFCC", fill_type="solid")
        self.RED_NOTE_FILL = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
        self.HEADER_FILL = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
        self.BORDER = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        
        #front excel headers
        self.summary_headers = [
            "Bank Code", "Bank Name", "Old Total", "New Total",
            "New Count", "Removed Count", "Unchanged Count", "Change %", "Notes"
        ]
        

    #============ PROCEDURES ==============
    @staticmethod
    def extract_date(text: str) -> str:
        if not isinstance(text, str):
            print("Invalid data type. Expected string.")
            return ""
    
        date_patterns = [r"(\d{2}[.\-/]+\d{2}[.\-/]+\d{4}",
                        r"\d{1,2}\s*(?:th|st|rd|nd)\s*[A-Za-z]+\s*\d{4}",
                        r"\d{2}[\.\-\/]+[A-Za-z]+[\.\-\/]+\d{4}",
                        r"\d{2}\s*[A-Za-z]+\s*\d{4})"]
        matches = re.findall(r"|".join(date_patterns), text, re.IGNORECASE)
        if matches:
            date_str = " ".join(matches)
            try:
                output_format = "%Y%m%d"
                dt_object = parse(date_str, fuzzy=True)
                return dt_object.strftime(output_format)
            except dateutil.parser._parser.ParserError as e:
                print(f"[ERROR]: {e}")
                return date_str
        return text
    
    def _boomerang(self,data): return data
    
    def _generate_hash_sha256(self,text:str)->str:
        if not isinstance(text,str):
            return f"{inspect.currentframe().f_code.co_name}: input non str"
        return hashlib.sha256(text.encode()).hexdigest()

    def _generate_hash_sha1(self,text:str)->str:
        if not isinstance(text,str):
            return f"{inspect.currentframe().f_code.co_name}: input non str"
        return hashlib.sha1(text.encode()).hexdigest()
    
    def _generalize_table_df(self,html_str)->str:
        cols = [f"column_{i}" for i in range(1, self.MAX_COLUMN_DF + 1)]
        dfs = pd.read_html(StringIO(html_str), flavor='html5lib')
        if not dfs: return pd.DataFrame(columns=cols)
        
        all_dfs = []
        for df in dfs:
            if df.empty: continue
            df = df.iloc[:, :self.MAX_COLUMN_DF].copy()  # truncate if too many columns
            df.columns = cols[:df.shape[1]]      # rename existing columns
            for i in range(df.shape[1], self.MAX_COLUMN_DF):  df[cols[i]] = "" # fill missing columns with empty strings
            df = df.reindex(columns=cols)        # ensure consistent column order
            all_dfs.append(df)

        final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=cols)
        return final_df.to_csv(index=False,header=False, sep='|', lineterminator='\n')
   
    def runner(self, data, function_to_execute):
        p_dict = data.copy()
        records = p_dict.get("records", [])

        for record in records:
            print(f">>Processing {record['bank_name']}")
            response_data = record.get("scraped_data", [])
            if not response_data:
                continue

            new_scraped_data = []

            for action in response_data:
                if not action.get("data_present"): continue
                response = action.get("response")
                if not response:  continue
                for _packet_ in response:
                    try:
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

                    except Exception as e:
                        error_msg = f"[ERROR] Failed to process packet for bank '{record['bank_name']}': {str(e)}"
                        if hasattr(self, "logger"):
                            self.logger.error(error_msg)
                        else:
                            print(error_msg)

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

            new_only_keys,removed_keys,unchanged_keys = new_keys - old_keys, old_keys - new_keys, old_keys & new_keys
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
    
    def _parse_table(self, entry):
        content_type = entry.get("type", "str")
        raw_content = entry.get("value", "")

        try:
            if content_type == "table_html":
                return pd.read_html(StringIO(raw_content))[0]

            elif content_type == "html":
                soup = BeautifulSoup(raw_content, "html.parser")
                text_lines = soup.get_text().splitlines()
                text_lines = [line.strip() for line in text_lines if line.strip()]
                return pd.DataFrame({"text": text_lines})

            elif content_type == "pdf":
                pdf_bytes = base64.b64decode(raw_content)
                pdf_file = BytesIO(pdf_bytes)
                all_rows = []
                try:
                    with pdfplumber.open(pdf_file) as pdf:
                        for page_num, page in enumerate(pdf.pages, start=1):
                            tables = page.extract_tables()
                            for table in tables:
                                all_rows.append([f"[Page {page_num}]"])
                                all_rows.extend(table)
                except Exception as e:
                    return pd.DataFrame([[f"Invalid PDF file: {e}"]])
                return pd.DataFrame(all_rows) if all_rows else pd.DataFrame([["No table found in PDF"]])

            elif content_type == "redir_pdf":
                pdf_bytes = base64.b64decode(raw_content)
                pdf_file = BytesIO(pdf_bytes)
                all_rows = []
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_output:
                    ocrmypdf.ocr(pdf_file, temp_output.name)
                try:
                    with pdfplumber.open(temp_output.name) as pdf:
                        for page_num, page in enumerate(pdf.pages, start=1):
                            tables = page.extract_tables()
                            for table in tables:
                                all_rows.append([f"[Page {page_num}]"])
                                all_rows.extend(table)
                except Exception as e:
                    return pd.DataFrame([[f"OCR PDF failed: {e}"]])
                return pd.DataFrame(all_rows) if all_rows else pd.DataFrame([["No table found in OCR PDF"]])

        except Exception as e:
            if hasattr(self, "logger") and self.logger:
                self.logger.error(f"Failed to parse data for content type {content_type}: {e}")
            else:
                print(f"[ERROR] Failed to parse data for content type {content_type}: {e}")
            return pd.DataFrame([[f"Failed to parse data of type: {content_type}"]])

    def _write_summary(self, ws, summary, start_row, bank_name="", bank_link=""):
        # Write bank name and link
        ws.cell(row=start_row, column=1, value="Bank Name")
        ws.cell(row=start_row, column=2, value=bank_name)
        ws.cell(row=start_row + 1, column=1, value="Bank Link")
        ws.cell(row=start_row + 1, column=2, value=bank_link)

        # Compute totals and change %
        new_count,removed_count,unchanged_count = summary.get("new_count", 0),summary.get("removed_count", 0),summary.get("unchanged_count", 0)
        old_total = removed_count + unchanged_count
        new_total = new_count + unchanged_count
        change_percent = round((new_count + removed_count) / max(old_total, 1) * 100, 2)

        # Write summary header
        ws.cell(row=start_row + 2, column=1, value="Summary")
        headers = ["Old Total", "New Total", "New Count", "Removed Count", "Unchanged Count", "Change %"]
        values = [old_total, new_total, new_count, removed_count, unchanged_count, change_percent]

        for col, header in enumerate(headers, start=1):
            ws.cell(row=start_row + 3, column=col, value=header)
        for col, val in enumerate(values, start=1):
            ws.cell(row=start_row + 4, column=col, value=val)

        return start_row + 6  # Next available row

    def _write_side_by_side_tables(self, ws, new_df, removed_df, start_row, title=None, gap=2):
        # --- Handle None or non-DataFrame inputs gracefully ---
        def to_df(data, placeholder):
            if data is None: return pd.DataFrame([[placeholder]])
            if isinstance(data, pd.DataFrame): return data if not data.empty else pd.DataFrame([[placeholder]])
            if isinstance(data, list):
                try: return pd.DataFrame(data) if data else pd.DataFrame([[placeholder]])
                except Exception: return pd.DataFrame([[placeholder]])
            return pd.DataFrame([[placeholder]])

        new_df,removed_df = to_df(new_df, "⚠️ No new data available"),to_df(removed_df, "⚠️ No old data available")

        # --- Layout setup ---
        new_col_start = 1
        removed_col_start = new_df.shape[1] + new_col_start + gap
        comparison_col_start = removed_col_start + removed_df.shape[1] + gap
        start_row += 3 

        # --- Write title (if any) ---
        if title:
            for i, line in enumerate(title):
                ws.cell(row=start_row + i, column=1, value=line)
            start_row += len(title)

        # --- Compute grid size ---
        max_rows = max(len(new_df), len(removed_df))
        max_cols = max(new_df.shape[1], removed_df.shape[1])

        # --- Write NEW Data Table ---
        for r_idx, row in enumerate(dataframe_to_rows(new_df, index=False, header=True)):
            for c_idx, val in enumerate(row):
                cell = ws.cell(row=start_row + r_idx, column=new_col_start + c_idx, value=val)
                cell.fill = self.SKY_BLUE_FILL

        # --- Write OLD Data Table ---
        for r_idx, row in enumerate(dataframe_to_rows(removed_df, index=False, header=True)):
            for c_idx, val in enumerate(row):
                cell = ws.cell(row=start_row + r_idx, column=removed_col_start + c_idx, value=val)
                cell.fill = self.GREY_FILL

        # --- Optional comparison logic (not essential now) ---
        for r in range(start_row + 1, start_row + max_rows + 1):
            for c in range(max_cols):
                comp_cell = ws.cell(row=r, column=comparison_col_start + c)
                new_col_letter = ws.cell(row=1, column=new_col_start + c).column_letter
                removed_col_letter = ws.cell(row=1, column=removed_col_start + c).column_letter
                comp_cell.value = f"={new_col_letter}{r}={removed_col_letter}{r}"

        for c in range(max_cols):
            col_letter = ws.cell(row=1, column=comparison_col_start + c).column_letter
            formula = f'{col_letter}{start_row + 1}=FALSE'
            ws.conditional_formatting.add(
                f'{col_letter}{start_row + 1}:{col_letter}{start_row + max_rows}',
                FormulaRule(formula=[formula], fill= self.RED_FILL)
            )

        start_row += max_rows + 2  # space after each block
        return start_row

    def generate_sorted_excel_report(self, comparison_json, output_path="DepositRate_Comparison_Report.xlsx"):
        sorted_records = sorted(
            comparison_json.get("records", []),
            key=lambda r: len(r.get("comparison_result", {}).get("new", [])),
            reverse=True
        )

        # === 3️⃣ Build Summary Sheet Data ===
        summary_data = []
        for record in sorted_records:
            bank_name,bank_code = record.get("bank_name"),record.get("bank_code")
            comparison_result = record.get("comparison_result", {})
            new_entries,removed_entries = comparison_result.get("new", []),comparison_result.get("removed", [])
            summary = comparison_result.get("summary", {})
            
            old_total = summary.get("old_total", 0)
            new_total = summary.get("new_total", 0)
            new_count = summary.get("new_count", 0)
            removed_count = summary.get("removed_count", 0)
            new_missing,old_missing = len(new_entries) == 0,len(removed_entries) == 0
      
            if new_missing and not old_missing: note = "⚠️ Scraping failed – No NEW data available"
            elif old_missing and not new_missing: note = "⚠️ No OLD data – First run or cache missing"
            elif new_missing and old_missing: note = "⚠️ No Data to Compare"
            else: note = "✓ Changes in Data"

            change_pct = 100.0 if old_total == 0 and new_count > 0 else \
                        round((new_count + removed_count) / old_total * 100, 2) if old_total else 0.0

            summary_data.append([
                bank_code, bank_name, old_total, new_total,
                new_count, removed_count, summary.get("unchanged_count", 0),
                min(change_pct, 100.0), note
            ])

        summary_df = pd.DataFrame(summary_data, columns=self.summary_headers)

        # === 4️⃣ Write Summary Sheet ===
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            ws_summary = writer.sheets["Summary"]

            # Auto column width
            for col in ws_summary.columns:
                max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
                ws_summary.column_dimensions[col[0].column_letter].width = max_length + 4

            # Header styling
            for cell in ws_summary[1]:
                cell.font = cell.font.copy(bold=True)
                cell.fill = self.HEADER_FILL
                cell.alignment = Alignment(horizontal="center", vertical="center")
            for row in ws_summary.iter_rows():
                for cell in row:
                    cell.border = self.BORDER

            # Conditional Formatting
            change_col = self.summary_headers.index("Change %") + 1
            change_range = f"{ws_summary.cell(row=2, column=change_col).coordinate}:{ws_summary.cell(row=len(summary_df)+1, column=change_col).coordinate}"
            ws_summary.conditional_formatting.add(change_range, CellIsRule(operator="greaterThanOrEqual", formula=["50"], fill=self.RED_FILL))
            ws_summary.conditional_formatting.add(change_range, CellIsRule(operator="between", formula=["20", "49.99"], fill=self.YELLOW_FILL))
            ws_summary.conditional_formatting.add(change_range, CellIsRule(operator="lessThan", formula=["20"], fill=self.GREEN_FILL))

            note_col = self.summary_headers.index("Notes") + 1
            note_range = f"{ws_summary.cell(row=2, column=note_col).coordinate}:{ws_summary.cell(row=len(summary_df)+1, column=note_col).coordinate}"
            ws_summary.conditional_formatting.add(note_range, FormulaRule(formula=[f'LEN({ws_summary.cell(row=2, column=note_col).coordinate})>0'], fill=self.RED_NOTE_FILL))

            # === 5️⃣ Write Per-Bank Sheets ===
            existing_sheets = set(writer.sheets.keys())

            for record in sorted_records:
                bank_name = record.get("bank_name")
                bank_code = record.get("bank_code")
                bank_link = record.get("base_url")
                comparison_result = record.get("comparison_result", {})
                summary = comparison_result.get("summary", {})
                new_entries = comparison_result.get("new", [])
                removed_entries = comparison_result.get("removed", [])

                sheet_name = f"{bank_name} ({bank_code})"[:31]
                counter = 1
                while sheet_name in existing_sheets:
                    suffix = f"_{counter}"
                    sheet_name = f"{bank_name[:31-len(suffix)]}{suffix}"
                    counter += 1
                existing_sheets.add(sheet_name)

                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]

                row_cursor = self._write_summary(ws, summary, start_row=1, bank_name=bank_name, bank_link=bank_link)
                max_tables = max(len(new_entries), len(removed_entries), 1)

                for i in range(max_tables):
                    new_entry = new_entries[i] if i < len(new_entries) else None
                    removed_entry = removed_entries[i] if i < len(removed_entries) else None

                    title = ""
                    if isinstance(new_entry, dict): title = new_entry.get("title", "")
                    elif isinstance(removed_entry, dict): title = removed_entry.get("title", "")
                    title = [title] if isinstance(title, str) else title or []

                    
                    # --- Unified, aligned side-by-side layout ---
                    new_df = self._parse_table(new_entry) if new_entry else pd.DataFrame([["⚠️ No new data available"]])
                    removed_df = self._parse_table(removed_entry) if removed_entry else pd.DataFrame([["⚠️ No old data available"]])

                    # Keep structure consistent: left = NEW, right = OLD
                    row_cursor = self._write_side_by_side_tables( ws,new_df, removed_df, start_row=row_cursor, title=title )


        print(f"✅ Excel comparison report generated successfully at {output_path}")
        return output_path

    