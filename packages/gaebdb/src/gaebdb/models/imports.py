from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

SCHEMA = "imports"


def utcnow() -> datetime:
    """Returns the current datetime (utc) when called."""
    return datetime.now(timezone.utc)


class LV(Base):
    __tablename__ = "lv"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True)
    project_name = Column(String, nullable=True)
    # Upload / Mandant / Fall referenzierbar
    external_ref = Column(String, nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    meta = Column(JSON, nullable=True)  # z.B. source files, raw infos

    titles = relationship(
        "Title",
        back_populates="lv",
        cascade="all, delete-orphan",
    )
    positions = relationship(
        "Position",
        back_populates="lv",
        cascade="all, delete-orphan",
    )


class Title(Base):
    __tablename__ = "title"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True)

    lv_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA}.lv.id", ondelete="CASCADE"),
        nullable=False,
    )

    parent_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA}.title.id", ondelete="CASCADE"),
        nullable=True,
    )

    name = Column(Text, nullable=False)
    level = Column(Integer, nullable=False, default=1)

    # Für schnelle Filter:
    gewerk_name = Column(Text, nullable=True)  # Top-Level
    untergewerk_name = Column(Text, nullable=True)  # 2. Ebene

    sort_index = Column(String, nullable=True)  # z.B. "01.", "01.01."

    lv = relationship("LV", back_populates="titles")
    parent = relationship("Title", remote_side=[id], backref="children")


class Position(Base):
    __tablename__ = "position"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True)

    lv_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA}.lv.id", ondelete="CASCADE"),
        nullable=False,
    )

    title_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA}.title.id", ondelete="CASCADE"),
        nullable=False,
    )

    oz = Column(String, nullable=False)  # "01.01.0001"
    gaeb_id = Column(String, nullable=True)  # Item-ID, falls vorhanden

    short_text = Column(Text, nullable=False)
    long_text = Column(Text, nullable=True)
    info = Column(Text, nullable=True)

    quantity = Column(Numeric(18, 6), nullable=False)
    unit = Column(String, nullable=False)  # QU / C62 / etc.

    unit_price_net = Column(Numeric(18, 6), nullable=True)
    total_price_net = Column(Numeric(18, 2), nullable=True)

    vat_rate = Column(Numeric(5, 2), nullable=True)

    # Denormalisierte Helfer:
    gewerk_name = Column(Text, nullable=True)
    untergewerk_name = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)

    lv = relationship("LV", back_populates="positions")
    title = relationship("Title")
