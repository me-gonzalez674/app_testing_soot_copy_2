# soot_tool/icartt.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

import pandas as pd


PathLike = Union[str, Path]


@dataclass
class ICARTTInfo:
    header_length: int
    ffi: str


@dataclass
class VariableDef:
    name: str
    unit: Optional[str] = None
    description: Optional[str] = None
    missing: Optional[float] = None


class ICARTTReader:
    """
    Best-effort ICARTT (.ict) reader.

    Assumptions aligned with your current convert_1.py:
    - line 1 contains: "<header_length>, <FFI>"
    - header row begins at line == header_length (1-indexed),
      so we skip header_length - 1 lines and let pandas read the next line as CSV header.
    - files are comma-separated; Latin-1 fallback.
    """

    def __init__(self, path: PathLike):
        self.path = Path(path)
        self.info = self._read_info()

    def _read_info(self) -> ICARTTInfo:
        # First line like: "39, 1001"
        first = self.path.read_text(errors="ignore", encoding="latin-1").splitlines()[0]
        parts = [p.strip() for p in first.split(",")]
        header_length = int(parts[0])
        ffi = parts[1] if len(parts) > 1 else ""
        return ICARTTInfo(header_length=header_length, ffi=ffi)

    def read_header_lines(self) -> List[str]:
        # Read first header_length lines
        lines = self.path.read_text(errors="ignore", encoding="latin-1").splitlines()
        return lines[: self.info.header_length]

    def _guess_missing_values(self) -> List[Union[str, float, int]]:
        """
        Heuristic: scan header for large sentinel values (-9999, 99999, etc).
        Mirrors your current approach. 
        """
        lines = self.read_header_lines()
        candidates: List[Union[str, float, int]] = []

        for ln in lines[: min(len(lines), 200)]:
            for tok in ln.replace(",", " ").split():
                if tok.startswith(("-", "+")) and tok[1:].isdigit():
                    val = int(tok)
                    if abs(val) >= 999:
                        candidates.append(val)

        seen = set()
        ordered = []
        for v in candidates:
            if v not in seen:
                seen.add(v)
                ordered.append(v)

        if not ordered:
            ordered = [-9999, -99999, -8888, 9999, 99999]

        return ordered

    def _guess_per_variable_missing(self) -> Dict[str, float]:
        # Keep minimal as you do now. :contentReference[oaicite:4]{index=4}
        return {}

    def read_table(
        self,
        *,
        na_values: Optional[List[Union[str, float, int]]] = None,
        strip_colnames: bool = True,
    ) -> pd.DataFrame:
        skiprows = max(self.info.header_length - 1, 0)

        if na_values is None:
            na_values = self._guess_missing_values()

        df = pd.read_csv(
            self.path,
            skiprows=skiprows,
            sep=",",
            encoding="latin-1",
            encoding_errors="ignore",
            engine="python",
            na_values=na_values,
        )

        if strip_colnames:
            df.columns = [str(c).strip() for c in df.columns]

        return df

    def read_metadata(self) -> Dict[str, str]:
        """
        Best-effort metadata extraction (matches your current 'safe(i)' approach). 
        """
        lines = self.read_header_lines()
        meta: Dict[str, str] = {}

        def safe(i: int) -> str:
            return lines[i].strip() if 0 <= i < len(lines) else ""

        meta["path"] = str(self.path)
        meta["header_length"] = str(self.info.header_length)
        meta["ffi"] = self.info.ffi

        meta["pi"] = safe(1)
        meta["organization"] = safe(2)
        meta["data_description"] = safe(3)
        meta["mission"] = safe(4)
        meta["volume_info"] = safe(5)
        meta["date_info"] = safe(6)
        meta["data_interval"] = safe(7)
        meta["independent_variable"] = safe(8)
        meta["seconds"] = safe(9)

        return {k: v for k, v in meta.items() if v}

    def read_variable_defs(self) -> List[VariableDef]:
        """
        Best-effort variable definitions for common FFI=1001 layout. 
        """
        lines = self.read_header_lines()
        if len(lines) < 11:
            return []

        try:
            n_dep = int(lines[9].strip())
        except Exception:
            return []

        start = 12
        block = lines[start : start + n_dep]
        out: List[VariableDef] = []

        for ln in block:
            parts = [p.strip() for p in ln.split(",")]
            if not parts:
                continue
            name = parts[0]
            unit = parts[1] if len(parts) > 1 else None
            desc = ",".join(parts[2:]).strip() if len(parts) > 2 else None
            out.append(VariableDef(name=name, unit=unit or None, description=desc or None))

        miss_map = self._guess_per_variable_missing()
        if miss_map:
            out = [VariableDef(v.name, v.unit, v.description, miss_map.get(v.name)) for v in out]

        return out