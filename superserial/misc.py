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
    """
    Stream absolute paths of all files within a directory tree starting at 'rootdir'.
    """
    for dirname, subdirlist, filelist in os.walk(rootdir):
        for fname in filelist:
            yield '{}/{}'.format(dirname, fname)


def pass_through(stuff):
    """
    Identity function.

    def pass_through(stuff):
        return stuff
    """
    return stuff


def encrypt_it(content, key):
    """
    Encrypts 'content' using 'key' as the key.

    This is a convenience function that wraps the Fernet class from the
    cryptography package.
    """
    print key
    ferob = Fernet(str(key))
    return ferob.encrypt(str(content))


def get_default_data_key(fpath=''.join([environ['HOME'],
                                        '/.defaultdatakey.txt'])):
    """
    Tries to load a key from a file at 'fpath'.

    fpath default: ~/.defaultdatakey.txt
    """
    try:
        with open(fpath) as keyfile:
            return [i for i in keyfile.readlines()][0].replace('\n', '')
    except(IOError):
        return None
