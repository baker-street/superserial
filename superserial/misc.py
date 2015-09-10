# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '7/13/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)


import os
from os import environ

from cryptography.fernet import Fernet


def spelunker_gen(rootdir):
    for dirname, subdirlist, filelist in os.walk(rootdir):
        for fname in filelist:
            yield '{}/{}'.format(dirname, fname)


def pass_through(stuff):
    return stuff


def encrypt_it(content, key):
    print key
    ferob = Fernet(str(key))
    return ferob.encrypt(str(content))


def get_default_data_key(fpath=''.join([environ['HOME'],
                                        '/.defaultdatakey.txt'])):
    try:
        with open(fpath) as keyfile:
            return [i for i in keyfile.readlines()][0].replace('\n', '')
    except(IOError):
        return None
