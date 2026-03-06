"""
Local file upload: detect type (and CSV attributes), save to temp.
CSV: first 10 data rows. JSON/XML/Parquet: first 8KB sample for dry run.
"""
import csv
import os
from io import StringIO


def save_upload_top10(file_storage, delimiter, temp_dir, has_header=True, end_of_record="\n", file_type="csv"):
    """
    Save uploaded file to temp_dir. For CSV: write first 10 data rows using delimiter;
    skip first row if has_header. For JSON/XML/Parquet: write first 8KB as sample.
    Returns (True, list of {name, path}) or (False, error_message).
    """
    try:
        data = file_storage.read()
        orig_name = file_storage.filename or "upload"
        if file_type == "csv":
            try:
                text = data.decode("utf-8")
            except Exception:
                text = data.decode("utf-8", errors="replace")
            reader = csv.reader(StringIO(text), delimiter=delimiter)
            rows = list(reader)
            if has_header and rows:
                data_rows = rows[0:11]
            else:
                data_rows = rows[:10]
            if not orig_name.lower().endswith(".csv"):
                orig_name += ".csv"
            fpath = os.path.join(temp_dir, orig_name)
            eol = end_of_record if end_of_record in ("\r\n", "\n") else "\n"
            with open(fpath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=delimiter, lineterminator=eol)
                for row in data_rows:
                    writer.writerow(row)
            return True, [{"name": os.path.basename(fpath), "path": fpath}]
        # JSON, XML, Parquet: write first 8KB as sample for dry run
        sample = data[:8192]
        ext = {"json": ".json", "xml": ".xml", "parquet": ".parquet"}.get(file_type, ".bin")
        if not orig_name.lower().endswith(ext.lstrip(".")):
            base = orig_name.rsplit(".", 1)[0] if "." in orig_name else orig_name
            name = base + ext
        else:
            name = orig_name
        fpath = os.path.join(temp_dir, name)
        with open(fpath, "wb") as f:
            f.write(sample)
        return True, [{"name": os.path.basename(fpath), "path": fpath}]
    except Exception as e:
        return False, str(e)
