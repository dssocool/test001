"""
Local file upload: save to temp and write first 10 rows to CSV for dry run.
"""
import csv
import os
from io import StringIO


def save_upload_top10(file_storage, delimiter, temp_dir):
    try:
        data = file_storage.read()
        try:
            text = data.decode("utf-8")
        except Exception:
            text = data.decode("utf-8", errors="replace")
        reader = csv.reader(StringIO(text), delimiter=delimiter)
        rows = []
        for i, row in enumerate(reader):
            if i >= 11:
                break
            rows.append(row)
        name = file_storage.filename or "upload.csv"
        if not name.lower().endswith(".csv"):
            name += ".csv"
        fpath = os.path.join(temp_dir, name)
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=delimiter)
            for row in rows:
                writer.writerow(row)
        return True, [{"name": os.path.basename(fpath), "path": fpath}]
    except Exception as e:
        return False, str(e)
