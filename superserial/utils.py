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
