import os
from datetime import datetime, timedelta
import shutil

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path

def filename_ru(base_name, count):
    now = datetime.now().strftime("%d.%m.%y")
    count_str = f"{count:,}".replace(",", ".")
    filename = f"{base_name} {now} - {count_str}.xlsx"
    return filename

def cleanup_old_files(data_dir, days=7):
    cutoff = datetime.now() - timedelta(days=days)
    for fname in os.listdir(data_dir):
        path = os.path.join(data_dir, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        except Exception:
            pass
