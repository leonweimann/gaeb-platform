from pathlib import Path
import sys
import pandas as pd
from gaebio.parse import GaebAdapter


def rows_from_lv(lv):
    rows = []

    # Helper, falls es Positionen direkt unter root gibt
    def emit_positions(parent_title, gewerk_name="", unter_name=""):
        for p in getattr(parent_title, "positions", []):
            rows.append(
                {
                    "Projekt": lv.project or "",
                    "OZ": p.oz,
                    "Gewerk": gewerk_name,
                    "Untergewerk": unter_name,
                    "Kurztext": p.short_text or "",
                    "Qty": str(p.quantity),
                    "QU": getattr(p.unit, "value", getattr(p.unit, "name", "")),
                    "Langtext": p.long_text or "",
                }
            )

    root = lv.root

    # 0) Positionen direkt am Root
    emit_positions(root, "", "")

    # 1) Gewerk-Ebene
    for t1 in getattr(root, "children", []):
        emit_positions(t1, t1.name or "", "")
        # 2) Untergewerk-Ebene
        for t2 in getattr(t1, "children", []):
            emit_positions(t2, t1.name or "", t2.name or "")

    return rows


def main():
    if len(sys.argv) < 2:
        print("Usage: try_parsing.py <file.X83|X84>")
        sys.exit(2)

    src = Path(sys.argv[1])
    phase = "X83" if src.suffix.upper() == ".X83" else "X84"
    out = src.with_suffix(f".{phase.lower()}.csv")

    with GaebAdapter(src) as ga:
        lv = ga.parse(phase)

    rows = rows_from_lv(lv)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"✅ CSV geschrieben: {out}")


if __name__ == "__main__":
    main()
