# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)


from os import getenv

# from six.moves.urllib.parse import urlsplit
import dataset
from sqlalchemy.exc import NoSuchTableError

from superserial.utils import get_default_data_key


class SQLStash(object):
    def __init__(self, uri,
                 table=getenv('METADATA_TABLE_NAME', 'metadata'),
                 chuncksize=500,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
                 # TODO (steven_c) consider removeing
                 index=False, indexcolumns=[['id', 'type'],
                                            ['id', 'generation'],
                                            ['id', 'parent'],
                                            ['id', 'use'],
                                            ['id'],
                                            ['type'],
                                            ['parent'],
                                            ['use'],
                                            ['generation'],
                                            ],
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

    def flush_the_stack(self):
        self.tbl.insert_many(rows=self.stack,
                             chunk_size=self.chuncksize)
        self.conn.commit()
        self.conn.begin
        self.stack = []

    def stash(self, datumdict):
        self.stack.append(datumdict)
        if len(self.stack) >= self.chuncksize:
            self.flush_the_stack()

    def close(self):
        self.flush_the_stack()
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
