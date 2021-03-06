# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '8/31/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)


import logging
LOG = logging.getLogger(__name__)

import pickle
import json
from collections import Iterable
import sys
import os
from os import path
from os import environ
from subprocess import call
import tempfile
import shutil
import subprocess
from uuid import uuid4
import time
import datetime
from getpass import getuser
from hashlib import md5
import pydoc

from dataset import connect, Database
import pathlib
from psycopg2 import connect as psyconnect
from psycopg2.extras import DictCursor
import dill
import yaml
try:
    from cryptography.fernet import Fernet
except ImportError:
    LOG.error('Library cryptography not installed, ' +
              'encryption will not be possible')

from higherorder.utils import xargs_cndm

from superserial.outsidemodules.smartopen import ParseUri

if sys.version_info[0] < 3:
    _STRINGTYPES = (basestring,)
else:
    _STRINGTYPES = (str, bytes)


DEFAULTDB = os.getenv('DATABASE_URL')
DEFAULTDB2 = os.getenv('DATABASE_URL2')


# ------------------------------------------------------------------------------
# Databases


def dataset_find_cndm(*morekeys, **xargs):
    defaultkeys = {'_limit', '_offset', '_step', 'order_by', 'return_count'}
    return xargs_cndm(*defaultkeys.union(set(morekeys)), **xargs)


# -------------------------------------
# PSQL


def psql_query(query, params={}, url=DEFAULTDB):
    with psyconnect(url) as con:
        curs = con.cursor(cursor_factory=DictCursor)
        curs.execute(query, params)
        return [dict(r) for r in curs]


def psql_query_no_return(query, params={}, url=DEFAULTDB):
    with psyconnect(url) as con:
        curs = con.cursor(cursor_factory=DictCursor)
        curs.execute(query, params)


def dataset_query(table, url=DEFAULTDB, **xargs):
    with Database(url=url) as db:
        return (r for r in db[table].find(**xargs))


# ------------------------------------------------------------------------------
# datetime
def unix_epoch():
    return unicode(int(time.time()))


def epoch2datetime(epoch):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(epoch)))


def datetime2epoch(dtime):
    return dtime.strftime('%s')


def utc_now():
    """
    current utc time
    """
    return datetime.datetime.utcnow()


def now():
    """
    current time
    """
    return datetime.datetime.now()


def utc_now_str():
    return utc_now().strftime('%d-%m-%Y_%Hh%Mm')


def now_str():
    return now().strftime('%d-%m-%Y_%Hh%Mm')

# ----------------------------------------


def uuid4_8_char():
    return unicode(uuid4()).split('-')[0]


def uuid4_12_char():
    return unicode(uuid4()).split('-')[-1]


def uuid4_full_char():
    return unicode(uuid4())


# Try to make the flatten funcs suck a little less; too many loops and what not.
def flatten_dict_tree(dicttree, __keypath=u''):
    """
    Flattens only the dicts in a dict tree.
    """
    newdict = {}
    for key, value in dicttree.items():
        fullkeypath = __keypath + '-' + key
        if isinstance(value, dict):
            newdict.update(flatten_dict_tree(value, fullkeypath))
        else:
            newdict[key] = value
    return newdict


def flatten_array_like_strct_gen(arraything, dictvalues=False):
    try:
        for i in arraything:
            if isinstance(i, _STRINGTYPES):
                yield i
            elif isinstance(i, dict):
                if dictvalues:
                    g = flatten_array_like_strct_gen(flatten_dict_tree(i).values(),
                                                     dictvalues=dictvalues)
                    for j in g:
                        yield j
                else:
                    yield i
            elif isinstance(i, Iterable):
                for j in flatten_array_like_strct_gen(i,
                                                      dictvalues=dictvalues):
                    yield j
            else:
                yield i
    except TypeError:
        yield arraything


# ------------------------------------------------------------------------------
# Serialized Utils

def pickle_save(filename_, object_):
    with open(filename_, 'wb') as handle:
        pickle.dump(object_, handle)


def pickle_load(filename_):
    with open(filename_, 'rb') as handle:
        return pickle.load(handle)


def pickle_save_iter_uni_to_utf8(file_, iterable):
    pickle_save(file_, set(i.encode('utf-8') for i in iterable))


def pickle_load_iter_utf8_to_uni(file_):
    return set(i.decode('utf-8') for i in pickle_load(file_))


def json_save(obj, file_, indent=None):
    with open(file_, 'w+') as fp:
        json.dump(obj, fp, indent=indent)


def json_load(file_):
    with open(file_) as fp:
        return json.load(fp)


def yaml_load(file_):
    with open(file_) as fp:
        return yaml.load(fp)


def dill_save(obj, file_, indent=None):
    with open(file_, 'w+') as fp:
        dill.dump(obj, fp, indent=indent)


def dill_load(file_):
    with open(file_) as fp:
        return dill.load(fp)


# ------------------------------------------------------------------------------
# File Utils

def spelunker_gen(rootdir):
    for dirname, subdirlist, filelist in os.walk(rootdir):
        for fname in filelist:
            yield path.join(dirname, fname)


def file_tail(file_, n=1, dropstuff='\n', replacewith=''):
    """
    Get the last n lines of a file.
    """
    return subprocess.check_output(['tail',
                                    '-' + str(n),
                                    file_
                                    ]).replace(dropstuff,
                                               replacewith).decode('utf-8')


class TempDir(object):
    def __init__(self, dirname=None):
        if dirname:
            self.dirname = dirname
        else:
            self.dirname = tempfile.mkdtemp()

    def __enter__(self):
        return self.dirname

    def __str(self):
        return self.dirname

    def __repr__(self):
        return self.dirname

    def __exit__(self, *args):
        try:
            os.removedirs(self.dirname)
        except OSError:
            shutil.rmtree(self.dirname)


def check_n_prep_path(filepath):
    if ParseUri(filepath).scheme in {'file'}:
        filepathobj = pathlib.Path(filepath)
        if not filepathobj.is_dir():
            filepathobj.mkdir(parents=True)


# ------------------------------------------------------------------------------
# Misc. Utils


def get_user_name():
    """
    uses getpass.getuser()
    """
    return getuser()


def hash_many(*many):
    return md5(''.join(many)).hexdigest()


def pass_through(stuff):
    return stuff


def encrypt_it(content, key):
    ferob = Fernet(str(key))
    return ferob.encrypt(str(content))


def get_default_data_key(fpath=''.join([environ['HOME'],
                                        '/.defaultdatakey.txt'])):
    try:
        with open(fpath) as keyfile:
            return [i for i in keyfile.readlines()][0].replace('\n', '')
    except(IOError):
        return None


def load_n_stream_docdicts_w_id(docpaths):
    for i, dpath in enumerate(docpaths):
        docdict = json_load(dpath)
        try:
            docdict['id']
        except KeyError:
            docid = dpath.split('/')[-1].split('.')[0]
            if not i % 50:
                LOG.debug('DocID: ' + str(docid))
            docdict['id'] = docid
        if not bool(docdict['id']):
            raise ValueError('docid is null: ' + str(docdict['id']))
        yield docdict


DOCPATHQUERY = "SELECT id,text_pointer FROM document WHERE use = 't';"


def load_n_stream_docdicts_w_id2(url=DEFAULTDB,
                                 q=DOCPATHQUERY,
                                 *_, **__):
    conn = connect(url)
    docs = ((doc['id'], json_load(doc['text_pointer']))
            for doc in conn.query(q))
    for docid, docdict in docs:
        docdict['id'] = docid
        yield docdict


FTRSETQUERY = 'SELECT id,doc_id,feature_array FROM featureset WHERE n > 0 SORT BY doc_id;'


def load_n_stream_feature_sets(url,
                               q=FTRSETQUERY,
                               *_, **__):
    conn = connect(url)
    ftrsets = ((ftrset['id'], ftrset['doc_id'], json_load(ftrset['feature_array']))
               for ftrset in conn.query(q))
    stack = []
    _docid = ''
    for ftrid, docid, ftrs in ftrsets:
        if docid != _docid:
            if stack:
                yield _docid, stack
                stack = []
                _docid = docid
        [stack.append(f) for f in ftrs]


def text_to_less(text, cmd='less -R'):
    pydoc.pipepager(text, cmd=cmd)


def edit_with_editor(text, editor='nano'):
    EDITOR = os.environ.get('EDITOR', editor)
    with tempfile.NamedTemporaryFile(suffix=".tmp") as tfile:
        tfile.write(text)
        tfile.flush()
        call([EDITOR, tfile.name])
        tfile.seek(0)
        return tfile.read()


def get_text_from_editor(editor='nano'):
    return edit_with_editor(text='', editor=editor)
