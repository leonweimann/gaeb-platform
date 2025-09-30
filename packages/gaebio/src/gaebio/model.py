from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Literal, Tuple
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone
from uuid import UUID, uuid4


Phase = Literal["X83", "X84"]
CENT = Decimal("0.01")


def money(x: Decimal) -> Decimal:
    return x.quantize(CENT, rounding=ROUND_HALF_UP)


class Unit(Enum):  # UNECE-Codes
    MTR = "m"
    MTK = "m^2"
    MTQ = "m^3"
    HUR = "h"
    C62 = "Stk"


_UNIT_ALIASES: Dict[str, Unit] = {
    "m": Unit.MTR,
    "meter": Unit.MTR,
    "lfdm": Unit.MTR,
    #
    "m2": Unit.MTK,
    "m^2": Unit.MTK,
    "m²": Unit.MTK,
    "qm": Unit.MTK,
    #
    "m3": Unit.MTQ,
    "m^3": Unit.MTQ,
    "m³": Unit.MTQ,
    "cbm": Unit.MTQ,
    #
    "h": Unit.HUR,
    "std": Unit.HUR,
    "stunden": Unit.HUR,
    #
    "stk": Unit.C62,
    "stück": Unit.C62,
    "st": Unit.C62,
}


def normalize_unit(raw: str, default: Unit = Unit.C62) -> Unit:
    key = raw.strip().lower().replace(".", "").replace(" ", "")
    return _UNIT_ALIASES.get(key, default)


def parse_oz(oz: Optional[str]) -> Tuple[int, ...]:
    """
    '1.2.10' -> (1,2,10); '1.2a' -> (1,2,0) konservativ.
    Leerstring/None -> ()
    """
    if not oz:
        return ()
    out: List[int] = []
    for tok in oz.replace(" ", "").split("."):
        if tok.isdigit():
            out.append(int(tok))
        else:
            digits = "".join(ch for ch in tok if ch.isdigit())
            out.append(int(digits) if digits else 0)
    return tuple(out)


@dataclass(slots=True)
class Position:
    id: UUID = field(default_factory=uuid4)
    oz: str = ""
    oz_path: Tuple[int, ...] = ()
    short_text: str = ""
    long_text: Optional[str] = None
    unit: Unit = Unit.C62
    quantity: Decimal = Decimal("0")
    unit_price_net: Optional[Decimal] = None
    vat_rate: Decimal = Decimal("0.19")

    def __post_init__(self):
        if not self.oz_path and self.oz:
            self.oz_path = parse_oz(self.oz)

    @property
    def total_price_net(self) -> Optional[Decimal]:
        if self.unit_price_net is None:
            return None
        return money(self.unit_price_net * self.quantity)

    @property
    def total_price_gross(self) -> Optional[Decimal]:
        if self.total_price_net is None:
            return None
        return money(self.total_price_net * (Decimal("1") + self.vat_rate))


@dataclass(slots=True)
class Title:
    id: UUID = field(default_factory=uuid4)
    name: str = ""
    oz: Optional[str] = None
    oz_path: Tuple[int, ...] = ()
    positions: List[Position] = field(default_factory=list)
    children: List["Title"] = field(default_factory=list)

    def __post_init__(self):
        if not self.oz_path and self.oz:
            self.oz_path = parse_oz(self.oz)

    def add_title(self, name: str, oz: Optional[str] = None) -> "Title":
        child = Title(name=name, oz=oz)
        self.children.append(child)
        return child

    def walk_titles(self) -> Iterable["Title"]:
        stack: List[Title] = [self]
        while stack:
            node = stack.pop()
            yield node
            # maintain original order
            stack.extend(reversed(node.children))

    def find_title_by_oz_path(self, path: Tuple[int, ...]) -> Optional["Title"]:
        for t in self.walk_titles():
            if t.oz_path == path:
                return t
        return None

    def iter_positions(self) -> Iterable[Position]:
        stack: List[Title] = [self]
        while stack:
            node = stack.pop()
            for p in node.positions:
                yield p
            # maintain original order
            stack.extend(reversed(node.children))

    @property
    def sum_net(self) -> Decimal:
        total = Decimal("0")
        for p in self.iter_positions():
            if p.total_price_net is not None:
                total += p.total_price_net
        return money(total)

    @property
    def sum_gross(self) -> Decimal:
        total = Decimal("0")
        for p in self.iter_positions():
            if p.total_price_gross is not None:
                total += p.total_price_gross
        return money(total)


@dataclass(slots=True)
class LV:
    id: UUID = field(default_factory=uuid4)
    phase: Phase = "X83"
    project: Optional[str] = None
    currency: str = "EUR"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    root: Title = field(default_factory=lambda: Title(name="LV"))
    meta: Dict[str, str] = field(default_factory=dict)
    default_vat_rate: Decimal = Decimal("0.19")

    def iter_positions(self) -> Iterable[Position]:
        return self.root.iter_positions()

    def add_title(self, parent: Title, *, name: str, oz: Optional[str] = None) -> Title:
        """Adds a new title to the given parent title and returns it."""
        return parent.add_title(name=name, oz=oz)

    def add_position(
        self,
        parent: Title,
        *,
        oz: str,
        short_text: str,
        quantity: Decimal,
        unit_raw: str | Unit = Unit.C62,
        unit_price_net: Optional[Decimal] = None,
        vat_rate: Optional[Decimal] = None,
        long_text: Optional[str] = None,
    ) -> Position:
        """Adds a new position to the given parent title and returns it."""
        unit = unit_raw if isinstance(unit_raw, Unit) else normalize_unit(unit_raw)
        pos = Position(
            oz=oz,
            oz_path=parse_oz(oz),
            short_text=short_text,
            long_text=long_text,
            unit=unit,
            quantity=quantity,
            unit_price_net=unit_price_net,
            vat_rate=vat_rate if vat_rate is not None else self.default_vat_rate,
        )
        parent.positions.append(pos)
        return pos

    def sort_by_oz(self):
        """
        Sorts the titles and positions recursive by their oz_path.

        Run this once after parsing is complete.
        """

        def sort_title(t: Title):
            t.children.sort(key=lambda c: c.oz_path)
            t.positions.sort(key=lambda p: p.oz_path)
            for c in t.children:
                sort_title(c)

        sort_title(self.root)
