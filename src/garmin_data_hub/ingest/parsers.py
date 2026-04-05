from __future__ import annotations
from pathlib import Path
from .parsers_fit import parse_fit, ParsedFitFile
# from .parsers_csv import parse_csv # CSV support temporarily disabled or needs update

def parse_activity_file(path: Path, lthr: int | None = None) -> ParsedFitFile:
    ext = path.suffix.lower()
    if ext == ".fit":
        return parse_fit(path, lthr=lthr)
    # if ext == ".csv":
    #     return parse_csv(path)
    raise ValueError(f"Unsupported file type: {ext}")
