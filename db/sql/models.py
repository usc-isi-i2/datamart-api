# type: ignore
# mypy complains about SQLAlchemy - a lot, so we disable it for this file

# These are the SQLAlchemy models for the Postgres database.
# These are supposed to move into a kgtk related package in the future. For now they are here
# because they are required by import_tsv.py (which should also be moved at some point)

import os
from os.path import abspath

from sqlalchemy import (Column, DateTime, ForeignKey, Integer, Numeric,
                        Sequence, String, create_engine, func)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class Edge(Base):
    __tablename__ = 'edges'
    id = Column(String, primary_key=True)
    node1 = Column(String, nullable=False, index=True)
    label = Column(String, nullable=False, index=True)
    node2 = Column(String, nullable=False, index=True)
    data_type = Column(String, nullable=False, index=True)

# We have satellite tables to the main statements table, which
# contain fields that are specific for each value type.abspath
#
# All these tables from a 1-to-0/1 relationship with the statements table

class StringValue(Base):
    __tablename__ = 'strings'
    edge_id = Column(String, ForeignKey('edges.id', ondelete="CASCADE", deferrable=True), primary_key=True)

    text = Column(String, nullable=False)
    language = Column(String, nullable=True)

class DateValue(Base):
    __tablename__ = 'dates'
    edge_id = Column(String, ForeignKey('edges.id', ondelete="CASCADE", deferrable=True), primary_key=True)

    date_and_time = Column(DateTime, nullable=False)
    precision = Column(String, nullable=True)
    calendar = Column(String, nullable=True)

class QuantityValue(Base):
    __tablename__ = 'quantities'
    edge_id = Column(String, ForeignKey('edges.id', ondelete="CASCADE", deferrable=True), primary_key=True)
    number = Column(Numeric, nullable=False)
    unit = Column(String, nullable=True)
    low_tolerance = Column(Numeric, nullable=True)
    high_tolerance = Column(Numeric, nullable=True)

class CoordinateValue(Base):
    __tablename__ = 'coordinates'
    edge_id = Column(String, ForeignKey('edges.id', ondelete="CASCADE", deferrable=True), primary_key=True)

    latitude = Column(Numeric, nullable=False)
    longitude = Column(Numeric, nullable=False)
    precision = Column(String, nullable=True)

class SymbolValue(Base):
    __tablename__ = 'symbols'
    edge_id = Column(String, ForeignKey('edges.id', ondelete="CASCADE", deferrable=True), primary_key=True)

    symbol = Column(String, nullable=False, index=True)

#    _lang_regex = re.compile("^'(.*)'@(.*)$")
#    def fill_facets(self):
#        lang = self._lang_regex.match(self.node2)
#        if lang:
#            self.o = lang.group(1)
#            self.node2_language = lang.group(2)
#            return
