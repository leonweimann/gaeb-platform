"""
GAEB Upload Client

Voraussetzungen:
- gaebio im PYTHONPATH
- gaebdb im PYTHONPATH
- GAEBDB_DSN_* + GAEBDB_TARGET konfiguriert
"""

import asyncio
import threading
import tkinter as tk
import traceback
from tkinter import filedialog, messagebox
from typing import Dict

from gaebdb import session_scope
from gaebdb.models.imports import LV as DbLV
from gaebdb.models.imports import Position as DbPosition
from gaebdb.models.imports import Title as DbTitle
from gaebio.parse import parse_x83, parse_x84


def get_node_key(node: object) -> int:
    """Stabiler Key für Title-Nodes basierend auf Objekt-Identität."""
    return id(node)


# ---------- Import-Logik (Mapping gaebio -> gaebdb) ----------


async def store_parsed_lv(
    parsed_lv, external_ref: str | None = None, price_index: dict | None = None
) -> DbLV:
    async with session_scope() as session:
        # Projektname / Meta
        project_name = getattr(parsed_lv, "project", None) or getattr(
            parsed_lv.root, "name", None
        )

        meta = getattr(parsed_lv, "meta", {}) or {}
        phase = getattr(parsed_lv, "phase", None)
        if phase and "phase" not in meta:
            meta["phase"] = phase

        db_lv = DbLV(
            project_name=project_name,
            external_ref=external_ref,
            meta=meta,
        )
        session.add(db_lv)
        await session.flush()

        # Title-Mapping: Objekt-Identität als Schlüssel
        title_map: Dict[int, DbTitle] = {}

        def walk(
            node,
            parent_db: DbTitle | None,
            level: int,
            gewerk_name: str | None,
            untergewerk_name: str | None,
        ):
            # Root → Kinder weiterreichen
            if getattr(node, "is_root", False):
                for child in getattr(node, "children", []):
                    walk(child, None, 1, None, None)
                return

            name = getattr(node, "name", "") or ""

            if level == 1:
                g_name = name
                u_name = None
            elif level == 2:
                g_name = gewerk_name or (parent_db.name if parent_db else None)
                u_name = name
            else:
                g_name = gewerk_name
                u_name = untergewerk_name

            sort_index = getattr(node, "oz", None) or getattr(node, "number", None)

            db_title = DbTitle(
                lv_id=db_lv.id,
                parent=parent_db,
                name=name,
                level=level,
                gewerk_name=g_name,
                untergewerk_name=u_name,
                sort_index=sort_index,
            )
            session.add(db_title)

            # Key über Objekt-Identität
            title_map[id(node)] = db_title

            for child in getattr(node, "children", []):
                walk(child, db_title, level + 1, g_name, u_name)

        root = parsed_lv.root
        for child in getattr(root, "children", []):
            walk(child, None, 1, None, None)

        # Fallback-Titel für verwaiste Positionen
        default_gewerk = DbTitle(
            lv_id=db_lv.id,
            parent=None,
            name="(Gewerklos)",
            level=1,
            gewerk_name="(Gewerklos)",
            untergewerk_name=None,
            sort_index=None,
        )
        session.add(default_gewerk)

        default_unter = DbTitle(
            lv_id=db_lv.id,
            parent=default_gewerk,
            name="(Untergewerklos)",
            level=2,
            gewerk_name="(Gewerklos)",
            untergewerk_name="(Untergewerklos)",
            sort_index=None,
        )
        session.add(default_unter)

        await session.flush()

        # Positionen einsammeln
        positions = getattr(parsed_lv, "positions", None)
        if positions is None:
            positions = []
            stack = [root]
            while stack:
                t = stack.pop()
                positions.extend(getattr(t, "positions", []))
                stack.extend(getattr(t, "children", []))

        for p in positions:
            parent = getattr(p, "parent", None)
            db_title = None

            if parent is not None:
                parent_key = get_node_key(parent)
                if parent_key:
                    db_title = title_map.get(parent_key)

            if db_title is None:
                db_title = default_unter

            unit = getattr(p, "unit_raw", None) or getattr(p, "unit", None) or "C62"

            key = (
                getattr(p, "gaeb_id", None) or str(getattr(p, "oz", "")).strip() or None
            )

            # Sicherstellen, dass wir immer ein Dict haben
            price_data: dict = {}
            if price_index is not None and key is not None:
                # .get(...) kann None liefern → mit "or {}" absichern
                price_data = price_index.get(key) or {}

            def pick(field: str, fallback):
                value = price_data.get(field)
                return value if value is not None else fallback

            db_pos = DbPosition(
                lv_id=db_lv.id,
                title_id=db_title.id,
                oz=str(getattr(p, "oz", "")),
                gaeb_id=getattr(p, "gaeb_id", None),
                short_text=getattr(p, "short_text", "") or "",
                long_text=getattr(p, "long_text", None),
                info=getattr(p, "info", None),
                quantity=getattr(p, "quantity", 0),
                unit=unit,
                unit_price_net=pick(
                    "unit_price_net", getattr(p, "unit_price_net", None)
                ),
                total_price_net=pick(
                    "total_price_net", getattr(p, "total_price_net", None)
                ),
                vat_rate=pick("vat_rate", getattr(p, "vat_rate", None)),
                gewerk_name=db_title.gewerk_name,
                untergewerk_name=db_title.untergewerk_name,
            )
            session.add(db_pos)

        return db_lv


def build_price_index(parsed_lv) -> dict[str, dict]:
    """
    Baut ein Mapping aus GAEB-Positionen (X84) auf Preisinfos.
    Key: gaeb_id oder oz (String)
    Value: Dict mit unit_price_net / total_price_net / vat_rate
    """
    root = parsed_lv.root
    positions = getattr(parsed_lv, "positions", None)
    if positions is None:
        positions = []
        stack = [root]
        while stack:
            t = stack.pop()
            positions.extend(getattr(t, "positions", []))
            stack.extend(getattr(t, "children", []))

    index: dict[str, dict] = {}
    for p in positions:
        key = getattr(p, "gaeb_id", None) or str(getattr(p, "oz", "")).strip()
        if not key:
            continue

        unit_price = getattr(p, "unit_price_net", None) or getattr(
            p, "unit_price", None
        )
        total_price = getattr(p, "total_price_net", None) or getattr(p, "amount", None)
        vat = getattr(p, "vat_rate", None)

        if unit_price is None and total_price is None and vat is None:
            continue

        index[key] = {
            "unit_price_net": unit_price,
            "total_price_net": total_price,
            "vat_rate": vat,
        }

    return index


async def import_gaeb(x83_path: str, x84_path: str | None, external_ref: str | None):
    parsed_x83 = parse_x83(x83_path)

    price_index = None
    if x84_path:
        parsed_x84 = parse_x84(x84_path)
        price_index = build_price_index(parsed_x84)

    db_lv = await store_parsed_lv(
        parsed_x83, external_ref=external_ref, price_index=price_index
    )
    return db_lv.id


# ---------- GUI-Client ----------


class UploadClient:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("GAEB Upload Client")

        # X83
        self.x83_var = tk.StringVar()
        tk.Label(root, text="X83-Datei (Pflicht):").grid(
            row=0, column=0, sticky="w", padx=8, pady=4
        )
        tk.Entry(root, textvariable=self.x83_var, width=50).grid(
            row=0, column=1, padx=8, pady=4
        )
        tk.Button(root, text="Auswählen", command=self.browse_x83).grid(
            row=0, column=2, padx=8, pady=4
        )

        # X84 (noch ohne Logik, aber UI lassen wir schon mal)
        self.x84_var = tk.StringVar()
        tk.Label(root, text="X84-Datei (optional):").grid(
            row=1, column=0, sticky="w", padx=8, pady=4
        )
        tk.Entry(root, textvariable=self.x84_var, width=50).grid(
            row=1, column=1, padx=8, pady=4
        )
        tk.Button(root, text="Auswählen", command=self.browse_x84).grid(
            row=1, column=2, padx=8, pady=4
        )

        # External Ref
        self.external_ref_var = tk.StringVar()
        tk.Label(root, text="External Ref (Projekt/Mandant):").grid(
            row=2, column=0, sticky="w", padx=8, pady=4
        )
        tk.Entry(root, textvariable=self.external_ref_var, width=50).grid(
            row=2, column=1, padx=8, pady=4
        )

        # Status
        self.status_var = tk.StringVar(value="Bereit.")
        tk.Label(root, textvariable=self.status_var, fg="grey").grid(
            row=3, column=0, columnspan=3, sticky="w", padx=8, pady=4
        )

        # Button
        tk.Button(root, text="Import starten", command=self.start_import).grid(
            row=4, column=0, columnspan=3, pady=10
        )

    def browse_x83(self):
        path = filedialog.askopenfilename(
            title="X83-Datei auswählen",
            filetypes=[("GAEB X83", "*.X83 *.x83"), ("Alle Dateien", "*.*")],
        )
        if path:
            self.x83_var.set(path)

    def browse_x84(self):
        path = filedialog.askopenfilename(
            title="X84-Datei auswählen",
            filetypes=[("GAEB X84", "*.X84 *.x84"), ("Alle Dateien", "*.*")],
        )
        if path:
            self.x84_var.set(path)

    def start_import(self):
        x83 = self.x83_var.get().strip()
        x84 = self.x84_var.get().strip() or None  # aktuell ungenutzt
        external_ref = self.external_ref_var.get().strip() or None

        if not x83:
            messagebox.showerror("Fehler", "Bitte eine X83-Datei auswählen.")
            return

        self.status_var.set("Import läuft...")
        self.root.update_idletasks()

        def worker():
            try:
                lv_id = asyncio.run(import_gaeb(x83, x84, external_ref))
                self._on_import_success(lv_id)
            except Exception as e:
                traceback.print_exc()
                self._on_import_error(e)

        threading.Thread(target=worker, daemon=True).start()

    def _on_import_success(self, lv_id: int):
        def update():
            self.status_var.set(f"Import erfolgreich. LV-ID: {lv_id}")
            messagebox.showinfo("Erfolg", f"Import erfolgreich.\nLV-ID in DB: {lv_id}")

        self.root.after(0, update)

    def _on_import_error(self, error: Exception):
        def update():
            self.status_var.set("Fehler beim Import.")
            messagebox.showerror(
                "Fehler", f"Beim Import ist ein Fehler aufgetreten:\n{error}"
            )

        self.root.after(0, update)


def main():
    root = tk.Tk()
    app = UploadClient(root)
    root.mainloop()


if __name__ == "__main__":
    main()
