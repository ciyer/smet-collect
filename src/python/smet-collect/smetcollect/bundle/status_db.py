#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
status_db.py

Module with the data base schema for the bundle status db.

Created by Chandrasekhar Ramakrishnan on 2015-09-25.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, BigInteger, Boolean
from sqlalchemy.orm import relationship, backref, sessionmaker

# --- The DB schema used to store the collector status ---
from sqlalchemy.sql import ClauseElement

Base = declarative_base()
Session = sessionmaker()


def get_or_create(session, model, defaults=None, **kwargs):
    """If a row with the properties is in the db, return it, otherwise create it.

    From http://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
    :param session:
    :param model:
    :param defaults:
    :param kwargs:
    :return: a tuple of (instance, did_create?)
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        return instance, True


class Race(Base):
    """ The representation of a race
    """
    __tablename__ = 'race'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    year = Column(Integer)
    slug = Column(String)
    active = Column(Boolean)


class Candidate(Base):
    """ The representation of a candidate
    """
    __tablename__ = 'candidate'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    party = Column(String)
    race_id = Column(Integer, ForeignKey('race.id'))
    race = relationship('Race', backref=backref('candidates', lazy='dynamic'))
    active = Column(Boolean)


class SearchTerm(Base):
    """ The representation of a term to search for
    """
    __tablename__ = 'search_term'

    id = Column(Integer, primary_key=True)
    term = Column(String)
    candidate_id = Column(Integer, ForeignKey('candidate.id'))
    candidate = relationship('Candidate', backref=backref('search_terms', lazy='dynamic'))
    active = Column(Boolean)

# TODO Introduce an Archive table and link it to run


class Run(Base):
    """ The representation of a search run
    """
    __tablename__ = 'run'

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('race.id'))
    race = relationship('Race', backref=backref('runs', lazy='dynamic'))
    start = Column(DateTime)
    end = Column(DateTime)
    # TODO Rename the results_folder to slug
    results_folder = Column(String)


class Search(Base):
    """ The representation of a search
    """
    __tablename__ = 'search'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    max_id = Column(BigInteger)
    earliest = Column(DateTime)
    latest = Column(DateTime)
    tweet_count = Column(Integer)
    results_path = Column(String)
    run_id = Column(Integer, ForeignKey('run.id'))
    run = relationship('Run', backref=backref('searches', lazy='dynamic'))
    search_term_id = Column(Integer, ForeignKey('search_term.id'))
    search_term = relationship('SearchTerm', backref=backref('searches', lazy='dynamic'))
