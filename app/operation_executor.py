import re,os, hashlib, inspect, dateutil, base64, pdfplumber, ocrmypdf, tempfile
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.parser import parse
from io import StringIO, BytesIO
from docx import Document
from pdf2docx import Converter
from docx import Document as DocxReader


from openpyxl.styles import PatternFill
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils.dataframe import dataframe_to_rows  
from openpyxl.formatting.rule import CellIsRule

from app.logger import get_global_logger

class OperationExecutor:
    
    def __init__(self, ):
        
        self.logger = get_global_logger()
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

    
        self.GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        self.YELLOW_FILL = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
        self.RED_FILL = PatternFill(start_color="F4CCCC", end_color="F4CCCC", fill_type="solid")
        self.RED_NOTE_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
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

    #Core Functionality
    # def runner(self, data, function_to_execute):
    #     p_dict = data.copy()
    #     records = p_dict.get("records", [])

    #     for record in records:
    #         # self.logger.info(f"Processing : {record['bank_name']}")
    #         print(f">>Processing {record['bank_name']}")
    #         response_data = record.get("scraped_data", [])
    #         if not response_data:
    #             continue

    #         new_scraped_data = []

    #         for action in response_data:
    #             if not action.get("data_present"):
    #                 continue

    #             response = action.get("response")
    #             if not response:
    #                 continue

    #             for _packet_ in response:
    #                 check_packet = _packet_.copy()

    #                 for stage_name, operations in function_to_execute.items():
    #                     for operation in operations:
    #                         # Unpack operation with optional expected_type
    #                         if len(operation) == 4:
    #                             func_name, source_key, target_key, expected_type = operation
    #                         else:
    #                             func_name, source_key, target_key = operation
    #                             expected_type = None

    #                         if target_key in check_packet:
    #                             raise ValueError(
    #                                 f"`target_key` cannot be similar to any of these keys: {list(check_packet.keys())}"
    #                             )

    #                         func = self.procedures.get(func_name)
    #                         if not func:
    #                             raise ValueError(f"Function '{func_name}' not found in procedures.")

    #                         input_value = _packet_.get(source_key)
    #                         if input_value is None:
    #                             continue

    #                         # Apply type check only in primary stage
    #                         if stage_name == "primary" and expected_type:
    #                             packet_type = _packet_.get("type", "").lower()
    #                             if isinstance(expected_type, list):
    #                                 if packet_type not in [t.lower() for t in expected_type]:
    #                                     continue
    #                             elif packet_type != expected_type.lower():
    #                                 continue

    #                         _packet_[target_key] = func(input_value)

    #                 new_scraped_data.append(_packet_)

    #         record["scraped_data"] = new_scraped_data

    #     return p_dict
        
    def runner(self, data, function_to_execute):
        p_dict = data.copy()
        records = p_dict.get("records", [])

        for record in records:
            print(f">>Processing {record['bank_name']}")
            response_data = record.get("scraped_data", [])
            if not response_data: continue
            new_scraped_data = []
            for action in response_data:
                if not action.get("data_present"):continue
                response = action.get("response")
                if not response:continue

                for _packet_ in response:
                    check_packet = _packet_.copy()

                    try:
                        for stage_name, operations in function_to_execute.items():
                            for operation in operations:
                                # Unpack operation with optional expected_type
                                if len(operation) == 4: func_name, source_key, target_key, expected_type = operation
                                else:
                                    func_name, source_key, target_key = operation
                                    expected_type = None

                                if target_key in check_packet: raise ValueError(f"`target_key` cannot be similar to any of these keys: {list(check_packet.keys())}")

                                func = self.procedures.get(func_name)
                                if not func: raise ValueError(f"Function '{func_name}' not found in procedures.")

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
                        if hasattr(self, "logger"): self.logger.error(error_msg)
                        else: print(error_msg)

                    new_scraped_data.append(_packet_)

            record["scraped_data"] = new_scraped_data

        return p_dict
    
    def process_comparison(self, old_json: dict, new_json: dict, key: str = "hash256") -> dict:
        def get_items(data, code):
            """Return scraped items for a specific bank code."""
            for record in data.get("records", []):
                if record.get("bank_code") == code:
                    return [item for item in record.get("scraped_data", []) if key in item]
            return []

        for new_rec in new_json.get("records", []):
            bank_code = new_rec.get("bank_code")

            old_items = get_items(old_json, bank_code)
            new_items = get_items(new_json, bank_code)

            old_keys, new_keys = {i[key] for i in old_items}, {i[key] for i in new_items}

            new_only = new_keys - old_keys
            removed = old_keys - new_keys
            same = old_keys & new_keys

            new_rec["comparison_result"] = {
                "new": [i for i in new_items if i[key] in new_only],
                "removed": [i for i in old_items if i[key] in removed],
                "unchanged": list(same),
                "summary": {
                    "old_total": len(old_items),
                    "new_total": len(new_items),
                    "new_count": len(new_only),
                    "removed_count": len(removed),
                    "unchanged_count": len(same)
                },
            }
            new_rec.pop("scraped_data", None)
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
        new_count = summary.get("new_count", 0)
        removed_count = summary.get("removed_count", 0)
        unchanged_count = summary.get("unchanged_count", 0)

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

        new_col_start = 1
        removed_col_start = new_df.shape[1] + new_col_start + gap
        comparison_col_start = removed_col_start + removed_df.shape[1] + gap

        SKY_BLUE_FILL = PatternFill(start_color="B3E5FC", end_color="B3E5FC", fill_type="solid")
        GREY_FILL = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
        RED_FILL = PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid")

        start_row += 3 

        if title:
            for i, line in enumerate(title):
                ws.cell(row=start_row + i, column=1, value=line)
            start_row += len(title)

        max_rows = max(len(new_df), len(removed_df))
        max_cols = max(new_df.shape[1], removed_df.shape[1])

        for r_idx, row in enumerate(dataframe_to_rows(new_df, index=False, header=True)):
            for c_idx, val in enumerate(row):
                cell = ws.cell(row=start_row + r_idx, column=new_col_start + c_idx, value=val)
                cell.fill = SKY_BLUE_FILL

        for r_idx, row in enumerate(dataframe_to_rows(removed_df, index=False, header=True)):
            for c_idx, val in enumerate(row):
                cell = ws.cell(row=start_row + r_idx, column=removed_col_start + c_idx, value=val)
                cell.fill = GREY_FILL

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
                FormulaRule(formula=[formula], fill=RED_FILL)
            )

        start_row += max_rows + 2  # Leave 2 blank rows below
        return start_row

    # def _write_single_table(self, ws, df, start_row, title=None):
    #     if title:
    #         for line in title:
    #             ws.cell(row=start_row, column=1, value=line)
    #             start_row += 1
    #     for r in dataframe_to_rows(df, index=False, header=True):
    #         for c_idx, value in enumerate(r, start=1):
    #             ws.cell(row=start_row, column=c_idx, value=value)
    #         start_row += 1
    #     return start_row
    
    def generate_sorted_excel_report(self, comparison_json, output_path="DepositRate_Comparison_Report.xlsx"):
        # Sort banks by number of new entries (most changed first)
        sorted_records = sorted(comparison_json.get("records", []),key=lambda r: len(r.get("comparison_result", {}).get("new", [])),reverse=True)
        summary_data = []

        # ==========================
        # Pass 1 → Build Summary Data
        # ==========================
        for record in sorted_records:
            bank_name = record.get("bank_name")
            bank_code = record.get("bank_code")
            comparison_result = record.get("comparison_result", {})
            summary = comparison_result.get("summary", {})
            new_entries = comparison_result.get("new", [])
            removed_entries = comparison_result.get("removed", [])

            old_total = summary.get("old_total", 0)
            new_total = summary.get("new_total", 0)
            new_count = summary.get("new_count", 0)
            removed_count = summary.get("removed_count", 0)

            # detect missing data
            new_missing = len(new_entries) == 0
            old_missing = len(removed_entries) == 0

            # base note
            if new_missing and not old_missing:
                note = "⚠️ Scraping failed – No NEW data available"
            elif old_missing and not new_missing:
                note = "⚠️ No OLD data – First run or cache missing"
            elif new_missing and old_missing:
                note = "⚠️ Both new & old missing – no data to compare"
            else:
                note = ""

            if old_total == 0:
                change_pct = 100.0 if new_count > 0 else 0.0
            else:
                change_pct = min(round((new_count + removed_count) / old_total * 100, 2), 100.0)


            summary_row = [
                bank_code, bank_name, old_total, new_total,
                new_count, removed_count, summary.get("unchanged_count", 0),
                change_pct, note
            ]
            summary_data.append(summary_row)

        summary_headers = [
            "Bank Code", "Bank Name", "Old Total", "New Total",
            "New Count", "Removed Count", "Unchanged Count", "Change %", "Notes"
        ]
        summary_df = pd.DataFrame(summary_data, columns=summary_headers)

        # ==========================
        # Pass 2 → Write to Excel
        # ==========================
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # ---- Write Summary Sheet ----
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            
            # Apply better styling
            for col in ws_summary.columns:
                max_length = 0
                column = col[0].column_letter  # Get the column name
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                ws_summary.column_dimensions[column].width = max_length + 4  # auto width

            # Bold header row
            for cell in ws_summary[1]:
                cell.font = cell.font.copy(bold=True)

            # Light gray header background
            from openpyxl.styles import PatternFill, Border, Side, Alignment
            header_fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
            thin_border = Border(
                left=Side(style='thin'), right=Side(style='thin'),
                top=Side(style='thin'), bottom=Side(style='thin')
            )
            for cell in ws_summary[1]:
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal="center", vertical="center")

            # Add borders to all cells
            for row in ws_summary.iter_rows():
                for cell in row:
                    cell.border = thin_border

            
            
            ws_summary = writer.sheets["Summary"]


            # Conditional formatting for % change
            change_col = summary_headers.index("Change %") + 1
            range_str = f"{ws_summary.cell(row=2, column=change_col).coordinate}:{ws_summary.cell(row=len(summary_df)+1, column=change_col).coordinate}"
            ws_summary.conditional_formatting.add(range_str, CellIsRule(operator="greaterThanOrEqual", formula=["50"], fill=self.RED_FILL))
            ws_summary.conditional_formatting.add(range_str, CellIsRule(operator="between", formula=["20", "49.99"], fill=self.YELLOW_FILL))
            ws_summary.conditional_formatting.add(range_str, CellIsRule(operator="lessThan", formula=["20"], fill=self.GREEN_FILL))

            # Conditional formatting for Notes (non-empty = red)
            note_col = summary_headers.index("Notes") + 1
            note_range = f"{ws_summary.cell(row=2, column=note_col).coordinate}:{ws_summary.cell(row=len(summary_df)+1, column=note_col).coordinate}"
            ws_summary.conditional_formatting.add(note_range, FormulaRule(formula=[f'LEN({ws_summary.cell(row=2, column=note_col).coordinate})>0'], fill=self.RED_NOTE_FILL))

            # ---- Write Each Bank Sheet ----
            for record in sorted_records:
                bank_name = record.get("bank_name")
                bank_code = record.get("bank_code")
                bank_link = record.get("base_url")
                comparison_result = record.get("comparison_result", {})
                summary = comparison_result.get("summary", {})
                new_entries = comparison_result.get("new", [])
                removed_entries = comparison_result.get("removed", [])

                new_missing = len(new_entries) == 0
                old_missing = len(removed_entries) == 0

                sheet_name = f"{bank_name} ({bank_code})"[:31]
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                row_cursor = self._write_summary(ws, summary, start_row=1,bank_name=bank_name, bank_link=bank_link)

                max_tables = max(len(new_entries), len(removed_entries), 1)

                for i in range(max_tables):
                    new_entry = new_entries[i] if i < len(new_entries) else None
                    removed_entry = removed_entries[i] if i < len(removed_entries) else None
                    title = new_entry.get("title") if new_entry else (removed_entry.get("title") if removed_entry else "")
                    title = [title] if isinstance(title, str) else title or []

                    # handle cases
                    if new_missing and removed_entry:
                        removed_df = self._parse_table(removed_entry)
                        row_cursor = self._write_side_by_side_tables(
                            ws,
                            pd.DataFrame([["--- Missing NEW Data ---"]]),
                            removed_df,
                            start_row=row_cursor,
                            title=title + ["⚠️ Scraper failed – using OLD data only"]
                        )

                    elif old_missing and new_entry:
                        new_df = self._parse_table(new_entry)
                        row_cursor = self._write_side_by_side_tables(
                            ws,
                            new_df,
                            pd.DataFrame([["--- Missing OLD Data ---"]]),
                            start_row=row_cursor,
                            title=title + ["⚠️ No old data available – first run"]
                        )

                    elif new_entry and removed_entry:
                        new_df = self._parse_table(new_entry)
                        removed_df = self._parse_table(removed_entry)
                        row_cursor = self._write_side_by_side_tables(
                            ws, new_df, removed_df, start_row=row_cursor, title=title
                        )

                    else:
                        ws.cell(row=row_cursor, column=1, value="⚠️ No data available for comparison")
                        row_cursor += 2

        return output_path


    
    #==============================================
    #==============================================
    @staticmethod
    def __parse_entry(entry):

        content_type = entry.get("type", "str")
        raw_content = entry.get("value", "")
        result = {"type": content_type, "content": [], "tables": [], "raw_text": ""}

        try:
            if content_type == "pdf":
                try:
                    pdf_bytes = base64.b64decode(raw_content)
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
                        tmp_pdf.write(pdf_bytes)
                        tmp_pdf_path = tmp_pdf.name

                    tmp_docx_path = tmp_pdf_path.replace(".pdf", ".docx")
                    converter = Converter(tmp_pdf_path)
                    converter.convert(tmp_docx_path, start=0, end=None)
                    converter.close()

                    with open(tmp_docx_path, "rb") as f:
                        docx_bytes = f.read()

                    os.remove(tmp_pdf_path)
                    os.remove(tmp_docx_path)

                    result["content"].append({"type": "docx","docx_bytes": docx_bytes})
                    result["raw_text"] = "[DOCX conversion successful]"

                except Exception as e:
                    result["content"].append({"type": "text","text": f"[PDF to DOCX failed: {e}]"})

            elif content_type == "html":
                soup = BeautifulSoup(raw_content, "html.parser")
                text = soup.get_text(separator="\n").strip()
                result["content"].append({"type": "text", "text": text})
                result["raw_text"] = text

            elif content_type == "table_html":
                df = pd.read_html(StringIO(raw_content))[0]
                result["content"].append({"type": "table", "table": df})
                result["tables"].append(df)
            
            elif content_type == "xlsx":
                xlsx_bytes = base64.b64decode(raw_content)
                xlsx_stream = BytesIO(xlsx_bytes)

                # Read all sheets
                sheets = pd.read_excel(xlsx_stream, sheet_name=None)

                for sheet_name, df in sheets.items():
                    result["content"].append({
                        "type": "table",
                        "sheet": sheet_name,
                        "table": df
                    })
                    result["tables"].append(df)

                result["raw_text"] = f"[XLSX with {len(sheets)} sheet(s) parsed]"
            else:
                result["content"].append({"type": "text", "text": f"[Unsupported Datatype: {content_type}]"})

        except Exception as e:
            result["content"].append({"type": "text", "text": f"[Failed to parse: {e}]"})

        return result
    
    @classmethod
    def generate_cache_doc_report(cls, comparison_json, output_path="DepositRate_Comparison_Report.docx"):
        document = Document()
        sorted_records = comparison_json.get("records", [])
        metadata = comparison_json.get("metadata", {})

        document.add_heading("Metadata Overview", level=1)
        document.add_paragraph("==============================================================")
        for key, value in metadata.items():
            document.add_paragraph(f"{key}: {value}")
        document.add_page_break()


        for record in sorted_records:
            bank_name = record.get("bank_name", "")
            bank_code = record.get("bank_code", "")
            scraped_data = record.get("scraped_data", [])

            document.add_heading(f">> {bank_code} : {bank_name}", level=2)
            document.add_paragraph("==============================================================")

            for scrape in scraped_data:
                responses = scrape.get("response", [])
                data_present = scrape.get("data_present", "")

                document.add_heading(
                    f"Action: {scrape.get("action", "")} | Timestamp: {scrape.get("timestamp", "")} | Present: {str(scrape.get("data_present", ""))} | Count: {scrape.get("response_count", "") if data_present else 0}",
                    level=3
                )
                document.add_paragraph(f"Website: {scrape.get("webpage", "")}")

                for response_entry in responses:
                    titles = response_entry.get("title", [])
                    titles = [titles] if isinstance(titles, str) else titles

                    parsed = cls.__parse_entry(response_entry)
                    content_stream = parsed["content"]

                    document.add_paragraph("-----------------------------------------------------")
                    for idx, line in enumerate(titles):
                        document.add_paragraph(f"TITLE {idx+1}: {line}")

                    for item in content_stream:
                        if item["type"] == "text":
                            document.add_paragraph(item["text"])
                            
                        elif item["type"] == "table":
                            df = item["table"]
                            table = document.add_table(rows=1, cols=len(df.columns))
                            table.style = 'Table Grid'
                            for i, col_name in enumerate(df.columns):
                                table.cell(0, i).text = str(col_name)
                            for _, row in df.iterrows():
                                row_cells = table.add_row().cells
                                for i, val in enumerate(row):
                                    row_cells[i].text = str(val)
                                    
                        elif item["type"] == "docx":
                            docx_bytes = item["docx_bytes"]
                            docx_stream = BytesIO(docx_bytes)
                            converted_doc = DocxReader(docx_stream)

                            for para in converted_doc.paragraphs:
                                if para.text.strip():
                                    document.add_paragraph(f"{para.text}")

                            for idx, tbl in enumerate(converted_doc.tables):
                                rows = tbl.rows
                                if rows:
                                    document.add_paragraph(f"TABLE {idx + 1}")
                                    table = document.add_table(rows=1, cols=len(rows[0].cells))
                                    table.style = 'Table Grid'
                                    for i, cell in enumerate(rows[0].cells):
                                        table.cell(0, i).text = cell.text.strip()
                                    for row in rows[1:]:
                                        row_cells = table.add_row().cells
                                        for i, cell in enumerate(row.cells):
                                            row_cells[i].text = cell.text.strip()
                                    document.add_paragraph("")
                                    document.add_paragraph("")             
                    
                        elif item["type"] == "xlsx":
                            # Decode the base64 string to bytes
                            xlsx_bytes = base64.b64decode(item["value"])
                            xlsx_stream = BytesIO(xlsx_bytes)

                            # Read all sheets
                            sheets = pd.read_excel(xlsx_stream, sheet_name=None)

                            for sheet_name, df in sheets.items():
                                document.add_paragraph(f"SHEET: {sheet_name}")

                                if df.empty:
                                    document.add_paragraph("[Empty sheet]")
                                    continue

                                table = document.add_table(rows=1, cols=len(df.columns))
                                table.style = 'Table Grid'

                                # Header row
                                for i, col in enumerate(df.columns):
                                    table.cell(0, i).text = str(col)

                                # Data rows
                                for _, row in df.iterrows():
                                    row_cells = table.add_row().cells
                                    for i, val in enumerate(row):
                                        row_cells[i].text = "" if pd.isna(val) else str(val)

                                document.add_paragraph("")  # spacing

            document.add_page_break()

        document.save(output_path)
        return output_path
    