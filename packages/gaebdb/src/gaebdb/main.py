from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from decimal import Decimal
from typing import Any, Optional

import psycopg
from dotenv import load_dotenv
from psycopg.rows import tuple_row

# Optional: Parser nur nutzen, wenn vorhanden
try:
    from gaebio.parse import parse_x83, parse_x84  # type: ignore
except Exception:
    parse_x83 = parse_x84 = None


# ───────────────────────── Helpers ─────────────────────────


def _d(v: Any) -> Optional[Decimal]:
    """Sanft auf Decimal casten, None bei Unklarheiten."""
    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _oz_path_str(path) -> Optional[str]:
    """oz_path als '1.2.3' stringifizieren (oder None)."""
    if not path:
        return None
    if isinstance(path, (list, tuple)):
        return ".".join(str(x) for x in path)
    return str(path)


DDL = """
CREATE TABLE IF NOT EXISTS lv (
  id UUID PRIMARY KEY,
  phase TEXT NOT NULL,
  project TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  meta JSONB
);

CREATE TABLE IF NOT EXISTS title (
  id UUID PRIMARY KEY,
  lv_id UUID NOT NULL REFERENCES lv(id) ON DELETE CASCADE,
  parent_id UUID REFERENCES title(id) ON DELETE CASCADE,
  oz_path TEXT,
  title_text TEXT,
  level INT,
  sort_index INT
);

CREATE TABLE IF NOT EXISTS position (
  id UUID PRIMARY KEY,
  lv_id UUID NOT NULL REFERENCES lv(id) ON DELETE CASCADE,
  title_id UUID REFERENCES title(id) ON DELETE CASCADE,
  oz_path TEXT,
  oz TEXT,
  short_text TEXT,
  long_text TEXT,
  unit TEXT,
  qty NUMERIC,
  unit_price NUMERIC,
  total_price_net NUMERIC
);
"""


def connect():
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL fehlt (z. B. in .env).", file=sys.stderr)
        sys.exit(2)
    return psycopg.connect(url, row_factory=tuple_row)


def _title_text(t) -> Optional[str]:
    for attr in ("title", "name", "text", "title_text"):
        v = getattr(t, attr, None)
        if isinstance(v, str):
            return v
    return None


def _walk_titles(root) -> list[tuple[Any, Optional[Any], int]]:
    out = []

    def rec(t, parent, level: int):
        out.append((t, parent, level))
        for c in getattr(t, "children", []) or []:
            rec(c, t, level + 1)

    rec(root, None, 0)
    return out


def _find_root(lv):
    root = getattr(lv, "root", None)
    if root is not None:
        return root
    titles = getattr(lv, "titles", None)
    if titles and isinstance(titles, list):
        return titles[0]
    return None


def _pos_fields(p) -> dict[str, Any]:
    qty = getattr(p, "qty", getattr(p, "quantity", None))
    up = getattr(p, "unit_price", getattr(p, "price_unit", None))
    tot = getattr(p, "total_price_net", getattr(p, "total", None))
    return {
        "oz_path": _oz_path_str(getattr(p, "oz_path", None)),  # ← String!
        "oz": getattr(p, "oz", None),
        "short_text": getattr(p, "short_text", getattr(p, "text", None)),
        "long_text": getattr(p, "long_text", getattr(p, "description", None)),
        "unit": getattr(p, "unit", None),
        "qty": _d(qty),
        "unit_price": _d(up),
        "total_price_net": _d(tot),
    }


def _title_key(t: Any) -> str:
    """Stabiler Key für Titel: bevorzugt oz_path-String, sonst Fallback auf Objekt-ID."""
    return _oz_path_str(getattr(t, "oz_path", None)) or f"obj-{id(t)}"


# ───────────────────────── Import X83/X84 ─────────────────────────


def insert_lv(conn, lv):
    """
    Schreibt ein LV inkl. Title/Positions-Baum in die DB.
    Erwartete Felder am LV: phase, project, meta (optional), root (Title)
    """
    lv_id = uuid.uuid4()
    phase = getattr(lv, "phase", None) or "X83"
    project = getattr(lv, "project", None)
    meta = getattr(lv, "meta", None)

    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            "INSERT INTO lv (id, phase, project, meta) VALUES (%s, %s, %s, %s)",
            (
                lv_id,
                str(phase),
                project,
                json.dumps(meta) if meta is not None else None,
            ),
        )

        root = _find_root(lv)
        title_id = {}
        t_count = p_count = 0

        if root is not None:
            flat = _walk_titles(root)
            for idx, (t, parent, level) in enumerate(flat):
                tid = uuid.uuid4()
                key = _title_key(t)
                title_id[key] = tid
                parent_id = title_id.get(_title_key(parent)) if parent else None

                cur.execute(
                    """INSERT INTO title (id, lv_id, parent_id, oz_path, title_text, level, sort_index)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        tid,
                        lv_id,
                        parent_id,
                        _oz_path_str(getattr(t, "oz_path", None)),
                        _title_text(t),
                        level,
                        idx,
                    ),
                )
                t_count += 1

                for p in getattr(t, "positions", []) or []:
                    pf = _pos_fields(p)
                    cur.execute(
                        """INSERT INTO position (id, lv_id, title_id, oz_path, oz, short_text, long_text, unit, qty, unit_price, total_price_net)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            uuid.uuid4(),
                            lv_id,
                            tid,
                            pf["oz_path"],
                            pf["oz"],
                            pf["short_text"],
                            pf["long_text"],
                            pf["unit"],
                            pf["qty"],
                            pf["unit_price"],
                            pf["total_price_net"],
                        ),
                    )
                    p_count += 1

    conn.commit()
    print(f"LV gespeichert: {lv_id}\n  Titles: {t_count}\n  Positionen: {p_count}")


# ───────────── Preise aus X84 auf vorhandene Positionen mappen ─────────────


def _collect_prices_from_lv(
    lv, update_key: str = "oz_path"
) -> list[tuple[str, Optional[Decimal], Optional[Decimal]]]:
    """
    Liefert Liste (key, unit_price, total_net) aus einem Preis-LV (z.B. X84).
    key = oz_path-String oder oz.
    """
    root = _find_root(lv)
    if root is None:
        return []
    out: list[tuple[str, Optional[Decimal], Optional[Decimal]]] = []

    for t, _, _ in _walk_titles(root):
        for p in getattr(t, "positions", []) or []:
            pf = _pos_fields(p)
            if update_key == "oz":
                key = str(pf["oz"]) if pf["oz"] is not None else None
            else:
                key = pf["oz_path"]  # punktierter String
            if key:
                out.append((key, pf["unit_price"], pf["total_price_net"]))
    return out


def apply_prices_from_lv(conn, price_lv, update_key: str = "oz_path"):
    """
    Schreibt Preise aus price_lv in bestehende position-Zeilen.
    Matching über update_key: 'oz_path' (default) oder 'oz'.
    Regel:
      - unit_price wird überschrieben, wenn vorhanden
      - total_price_net = bevorzugt vom Preis-LV; sonst qty * unit_price
    """
    rows = _collect_prices_from_lv(price_lv, update_key=update_key)
    if not rows:
        print("Keine Preis-Daten gefunden (price_lv leer?).")
        return

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_price_map;")
        cur.execute("""
            CREATE TEMP TABLE tmp_price_map (
                key TEXT PRIMARY KEY,
                unit_price NUMERIC,
                total_net NUMERIC
            ) ON COMMIT DROP;
        """)

        cur.executemany(
            "INSERT INTO tmp_price_map (key, unit_price, total_net) VALUES (%s, %s, %s)",
            rows,
        )

        # unit_price aktualisieren
        cur.execute(
            """
            UPDATE position p
            SET unit_price = COALESCE(t.unit_price, p.unit_price)
            FROM tmp_price_map t
            WHERE (CASE WHEN %s = 'oz_path' THEN p.oz_path ELSE p.oz END) = t.key;
            """,
            (update_key,),
        )

        # total_price_net aktualisieren
        cur.execute(
            """
            UPDATE position p
            SET total_price_net = COALESCE(
                t.total_net,
                CASE
                    WHEN p.qty IS NOT NULL AND p.unit_price IS NOT NULL
                    THEN ROUND(p.qty * p.unit_price, 2)
                    ELSE p.total_price_net
                END
            )
            FROM tmp_price_map t
            WHERE (CASE WHEN %s = 'oz_path' THEN p.oz_path ELSE p.oz END) = t.key;
            """,
            (update_key,),
        )

    conn.commit()
    print(f"Preise angewendet: {len(rows)} Keys über '{update_key}' gemappt.")


# ──────────────────────────────── CLI ──────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(description="GAEB-DB Tools (Neon/Postgres).")
    ap.add_argument("--gaeb", help="Pfad zu .X83/.X84 (LV importieren).")
    ap.add_argument(
        "--phase", default="X83", help="Phase-Label beim Import (nur mit --gaeb)."
    )
    ap.add_argument("--project", default=None, help="Projektname (nur mit --gaeb).")
    ap.add_argument(
        "--apply-prices-from",
        dest="apply_prices_from",
        help="Pfad zu .X84 (Preise in DB übertragen).",
    )
    ap.add_argument(
        "--update-key",
        choices=["oz_path", "oz"],
        default="oz_path",
        help="Matching-Schlüssel für Preis-Update.",
    )
    args = ap.parse_args()

    conn = connect()

    did_something = False

    if args.gaeb:
        if parse_x83 is None and parse_x84 is None:
            print("gaebio.parse nicht verfügbar. `uv add gaebio`?", file=sys.stderr)
            sys.exit(2)

        path = args.gaeb
        with open(path, "rb") as f:
            data = f.read()

        ext = os.path.splitext(path)[1].lower()
        if ext in (".x84", ".84"):
            if parse_x84 is None:
                print("parse_x84 nicht verfügbar.", file=sys.stderr)
                sys.exit(2)
            lv = parse_x84(data)
        else:
            if parse_x83 is None:
                print("parse_x83 nicht verfügbar.", file=sys.stderr)
                sys.exit(2)
            lv = parse_x83(data)

        # Phase/Project sanft setzen, falls leer
        if getattr(lv, "phase", None) is None and args.phase:
            try:
                setattr(lv, "phase", args.phase)
            except Exception:
                pass
        if getattr(lv, "project", None) is None and args.project:
            try:
                setattr(lv, "project", args.project)
            except Exception:
                pass

        insert_lv(conn, lv)
        did_something = True

    if args.apply_prices_from:
        if parse_x84 is None and parse_x83 is None:
            print("gaebio.parse nicht verfügbar. `uv add gaebio`?", file=sys.stderr)
            sys.exit(2)
        p = args.apply_prices_from
        with open(p, "rb") as f:
            pdata = f.read()
        # Normalerweise X84, aber wir erlauben beides
        ext = os.path.splitext(p)[1].lower()
        price_lv = None
        if ext in (".x84", ".84"):
            if parse_x84 is None:
                print("parse_x84 nicht verfügbar.", file=sys.stderr)
                sys.exit(2)
            price_lv = parse_x84(pdata)
        else:
            if parse_x83 is None:
                print("parse_x83 nicht verfügbar.", file=sys.stderr)
                sys.exit(2)
            price_lv = parse_x83(pdata)

        apply_prices_from_lv(conn, price_lv, update_key=args.update_key)
        did_something = True

    # Nichts angegeben? Nur Schema bereitstellen.
    if not did_something:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        print("DB-Schema angelegt (lv/title/position).")


if __name__ == "__main__":
    main()
