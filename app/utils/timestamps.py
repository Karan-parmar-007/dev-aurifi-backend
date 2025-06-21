from datetime import datetime

def add_timestamps(doc: dict, is_update: bool = False) -> dict:
    now = datetime.now()
    if not is_update:
        doc["created_at"] = now
    doc["updated_at"] = now
    return doc
