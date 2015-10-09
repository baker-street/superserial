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
from os import environ
import tempfile
import shutil
import subprocess

import yaml
try:
    from cryptography.fernet import Fernet
except ImportError:
    LOG.error('Library cryptography not installed, ' +
              'encryption will not be possible')


# ------------------------------------------------------------------------------
# Misc

if sys.version_info[0] < 3:
    _STRINGTYPES = (basestring,)
else:
    _STRINGTYPES = (str, bytes)


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


# ------------------------------------------------------------------------------
# File Utils

def spelunker_gen(rootdir):
    for dirname, subdirlist, filelist in os.walk(rootdir):
        for fname in filelist:
            yield '{}/{}'.format(dirname, fname)


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


# ------------------------------------------------------------------------------
# Misc. Utils

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
