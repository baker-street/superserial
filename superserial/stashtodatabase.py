# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)

import logging
LOG = logging.getLogger(__name__)

from os import getenv

import dataset
from sqlalchemy.exc import NoSuchTableError

from superserial.utils import get_default_data_key


class SQLStash(object):
    def __init__(self, uri=getenv('DATABASE_URL'),
                 table=getenv('STASH_TABLE_NAME'),
                 chuncksize=20,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
                 # TODO (steven_c) consider removeing
                 index=False, indexcolumns=None,
                 **xargs):
        self.uri = uri
        self.chuncksize = chuncksize
        self.table = table
        self.index = index
        self.indexcolumns = indexcolumns
        self.conn = dataset.connect(uri, reflect_metadata=False,
                                    engine_kwargs={'pool_recycle': 3600,
                                                   'convert_unicode': True,
                                                   'encoding': 'utf-8',
                                                   })
        self.conn.begin()
        try:
            self.tbl = self.conn.load_table(self.table)
        except NoSuchTableError:
            self.tbl = self.conn.get_table(self.table,
                                           primary_id='id',
                                           primary_type='String(36)')
        if bool(self.index) and bool(self.indexcolumns):
            if isinstance(self.indexcolumns[0], basestring):
                self.tbl.create_index(columns=self.indexcolumns)
            elif isinstance(self.indexcolumns[0], list):
                for ic in self.indexcolumns:
                    self.tbl.create_index(columns=ic)
        self.stack = []

    def flush_the_stack(self, chunk_size=None):
        if not chunk_size:
            chunk_size = self.chuncksize
        self.tbl.insert_many(rows=self.stack,
                             chunk_size=chunk_size)
        self.conn.commit()
        self.conn.begin
        self.stack = []

    def stash(self, datumdict):
        self.stack.append(datumdict)
        if len(self.stack) >= self.chuncksize:
            self.flush_the_stack()

    def close(self):
        LOG.debug('SQLStash, close has been called')
        stacksize = len(self.stack) / 2
        self.flush_the_stack(chunk_size=stacksize)
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        LOG.debug('SQLStash, exit has been called')
        self.close()
