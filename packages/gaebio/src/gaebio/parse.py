from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal, Optional, Union

import pandas as pd

from .model import (
    LV,
    Title,
)

try:
    from gaeb_parser import XmlGaebParser

    # Monkeypatch for None-safe OZ-concatenation
    if hasattr(XmlGaebParser, "_parse_item"):
        _original_parse_item = XmlGaebParser._parse_item

        def _parse_item_safe(self, item_soup, level):
            if hasattr(self, "oz") and isinstance(self.oz, list):
                self.oz = [
                    o if isinstance(o, str) else (str(o) if o is not None else "")
                    for o in self.oz
                ]
            return _original_parse_item(self, item_soup, level)

        XmlGaebParser._parse_item = _parse_item_safe

except ImportError:
    XmlGaebParser = None


def _to_decimal(v: object | None) -> Optional[Decimal]:
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if s == "":
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _clean_text(s: object | None) -> str:
    if s is None:
        return ""
    # Normalize whitespace
    text = str(s).replace("\u00a0", " ").replace("\u202f", " ")
    return " ".join(text.split())


def _detect_gaeb_meta_from_file(
    path: Union[str, Path],
) -> tuple[dict[str, str], str]: ...


@dataclass(slots=True)
class _TempFile:
    """Manages a temporary file that is deleted on cleanup."""

    path: Optional[Path] = None

    def write(self, data: bytes, suffix: str = ".xml") -> Path:
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        try:
            temp.write(data)
            temp.flush()
        finally:
            temp.close()
        self.path = Path(temp.name)
        return self.path

    def cleanup(self):
        if self.path and self.path.exists():
            try:
                os.remove(self.path)
            finally:
                self.path = None


class GaebAdapter:
    """
    Adapter to convert from gaeb_parser's data structures to gaebio's data structures.
    """

    REQUIRED_COLUMNS = [
        "Projekt",
        "OZ",
        "Gewerk",
        "Untergewerk",
        "Kurztext",
        "Qty",
        "QU",
        "TLK",
        "Langtext",
        "Info",
    ]

    def __init__(self, source: Union[str, Path, bytes]):
        if XmlGaebParser is None:
            raise RuntimeError("gaeb_parser is not installed / importable")

        self._temp_file = _TempFile()
        if isinstance(source, (str, Path)):
            self._file_path = Path(source)
        elif isinstance(source, (bytes, bytearray)):
            self._file_path = self._temp_file.write(bytes(source))
        else:
            raise TypeError("source must be str | Path | bytes")

        if not self._file_path.exists():
            raise FileNotFoundError(f"File not found: {self._file_path}")

        self._parser = XmlGaebParser(str(self._file_path))
        # Set Project name if available
        self._project_name = getattr(self._parser, "project_name", None)

    def __enter__(self) -> "GaebAdapter":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._temp_file.cleanup()

    def parse(self, phase: Literal["X83", "X84"] = "X83") -> LV:
        """
        Parses the file with the `XmlGaebParser` into our data structures.
        All positions will be placed under the root title (no auto-clustering).

        Hierarchy: Root -> Gewerk -> Untergewerk. Positions are added to the corresponding Untergewerk titles.
        """
        df = self._load_df()
        lv = LV(phase=phase, meta={"source": str(self._file_path)})

        # Name root, if possible
        project_name = _clean_text(self._project_name) or _clean_text(
            self._first_or_blank(df, "Projekt")
        )
        if project_name:
            lv.project = project_name
            lv.root.name = project_name

        # Title-cache: {{gewerk, untergewerk}: Title}
        title_cache: dict[tuple[str, str], Title] = {}

        def ensure_title(gewerk: str, unter: str) -> Title:
            key = (gewerk, unter)
            if key in title_cache:
                return title_cache[key]
            # Level 1: Gewerk
            t1 = None
            for t in lv.root.children:
                if _clean_text(getattr(t, "name", "")) == gewerk:
                    t1 = t
                    break
            if t1 is None:
                t1 = lv.add_title(lv.root, name=gewerk or "(Gewerklos)")
            # Level 2: Untergewerk
            t2 = None
            for t in t1.children:
                if _clean_text(getattr(t, "name", "")) == unter:
                    t2 = t
                    break
            if t2 is None:
                t2 = lv.add_title(t1, name=unter or "(Untergewerklos)")
            title_cache[key] = t2
            return t2

        # Rows -> Positions
        for _, row in df.iterrows():
            oz = _clean_text(row.get("OZ"))
            gewerk = _clean_text(row.get("Gewerk"))
            unter = _clean_text(row.get("Untergewerk"))
            short = _clean_text(row.get("Kurztext"))
            long = _clean_text(row.get("Langtext"))
            qty = _to_decimal(row.get("Qty")) or Decimal("0")
            qu_raw = _clean_text(row.get("QU")) or "C62"  # 'Unit'

            unit_price = None  # TODO: To be implemented later
            vat_rate = None  # TODO: To be implemented later

            parent = ensure_title(gewerk, unter)
            lv.add_position(
                parent=parent,
                oz=oz,
                short_text=short,
                long_text=long or None,
                quantity=qty,
                unit_raw=qu_raw,
                unit_price_net=unit_price,
                vat_rate=vat_rate,
            )

        lv.sort_by_oz()
        return lv

    def _load_df(self) -> pd.DataFrame:
        if not hasattr(self._parser, "get_df"):
            raise RuntimeError("gaeb_parser.XmlGaebParser does not support get_df()")
        df = self._parser.get_df()
        if not isinstance(df, pd.DataFrame):
            raise RuntimeError(
                "gaeb_parser.XmlGaebParser.get_df() did not return a pandas.DataFrame"
            )

        # Add missing columns with empty values
        for column in self.REQUIRED_COLUMNS:
            if column not in df.columns:
                df[column] = ""

        # Keep only required columns and fill NaN with empty strings
        df = df[self.REQUIRED_COLUMNS].fillna("")
        return df

    @staticmethod
    def _first_or_blank(df: pd.DataFrame, column: str) -> str:
        try:
            if column in df.columns and len(df[column]) > 0:
                value = df[column].iloc[0]
                return "" if pd.isna(value) else str(value)
        except Exception:
            pass
        return ""


def parse_x83(source: Union[str, Path, bytes]) -> LV:
    """
    Parses a GAEB X83 file (DA11 XML format) into our data structures.
    All positions will be placed under the root title (no auto-clustering).

    Hierarchy: Root -> Gewerk -> Untergewerk. Positions are added to the corresponding Untergewerk titles.

    :param source: Path to the GAEB X83 file or bytes of the file content.
    :return: Parsed LV object.
    """
    return __parse_with_adapter(source=source, phase="X83")


def parse_x84(source: Union[str, Path, bytes]) -> LV:
    """
    Parses a GAEB X84 file (DA11 XML format) into our data structures.
    All positions will be placed under the root title (no auto-clustering).

    Hierarchy: Root -> Gewerk -> Untergewerk. Positions are added to the corresponding Untergewerk titles.

    :param source: Path to the GAEB X84 file or bytes of the file content.
    :return: Parsed LV object.
    """
    return __parse_with_adapter(source=source, phase="X84")


def __parse_with_adapter(
    source: Union[str, Path, bytes], phase: Literal["X83", "X84"]
) -> LV:
    with GaebAdapter(source) as adapter:
        return adapter.parse(phase=phase)
