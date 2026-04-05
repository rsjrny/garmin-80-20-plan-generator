from __future__ import annotations
import hashlib
from pathlib import Path

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def stat_signature(path: Path) -> tuple[int, int]:
    st = path.stat()
    return int(st.st_size), int(st.st_mtime_ns)
