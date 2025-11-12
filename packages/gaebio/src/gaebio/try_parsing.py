from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Literal

from gaebio.model import LV
from gaebio.parse import parse_x83, parse_x84


def lv_to_rows(lv: LV) -> list[dict]:
    rows: list[dict] = []
    for pos in lv.iter_positions():
        rows.append(
            {
                "phase": lv.phase,
                "project": lv.project or "",
                "gaeb_id": getattr(pos, "gaeb_id", "") or "",
                "oz": pos.oz,
                "oz_path": ".".join(str(x) for x in pos.oz_path),
                "short_text": pos.short_text,
                "long_text": (pos.long_text or "").replace("\n", " "),
                "unit": pos.unit.value,
                "quantity": str(pos.quantity),
                "unit_price_net": (
                    str(pos.unit_price_net) if pos.unit_price_net is not None else ""
                ),
                "total_price_net": (
                    str(pos.total_price_net) if pos.total_price_net is not None else ""
                ),
            }
        )
    return rows


def parse_file(path: Path, phase: Literal["X83", "X84"]) -> LV:
    if phase == "X83":
        return parse_x83(path)
    return parse_x84(path)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: try_parsing.py <gaeb-file> [X83|X84]")
        raise SystemExit(1)

    src = Path(sys.argv[1])
    phase = "X83"
    if len(sys.argv) >= 3 and sys.argv[2].upper() == "X84":
        phase = "X84"

    lv = parse_file(src, phase=phase)
    rows = lv_to_rows(lv)
    out_csv = src.with_suffix(f".{phase.lower()}.csv")

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Parsed {src.name}")
    print(f"   Phase:        {lv.phase}")
    print(f"   Project:      {lv.project}")
    print(f"   Positions:    {len(rows)}")
    if phase == "X84":
        with_prices = sum(1 for r in rows if r["unit_price_net"])
        print(f"   With prices:  {with_prices}")
    print(f"   Output (csv): {out_csv}")


if __name__ == "__main__":
    main()
