# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '8/31/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from superserial import(__title__, __version__, __status__, __license__,
                        __maintainer__, __email__)


import sys
import logging
LOG = logging.getLogger(__name__)
LOGFMT = '''%(levelname)s\tproc:%(process)d thread:%(thread)d module:%(module)s\
\t%(message)s'''

from superserial.stashenmasse import EnMasseStash
from superserial import Gula


class TestStashObj(object):
    def __init__(self, **xargs):
        LOG.info('TestStashObj initiated')

    def stash(self, datumdict):
        LOG.info('TestStashObj stash called, given:\t' + str(datumdict))

    def close(self):
        LOG.info('TestStashObj close called')

    def __enter__(self):
        LOG.info('TestStashObj __enter__ called')
        return self

    def __exit__(self, type, value, traceback):
        LOG.info('TestStashObj __exit__ called')
        self.close()


def test_EnMasseStash_basic_with_TestStashObj():
    testinst = EnMasseStash(stashobjdict={'test': TestStashObj(foo='bar')})
    testinst.stash({'test': 'foo'})
    testinst.close()


def test_EnMasseStash_basic_with_TestStashObj_as_context_mnger():
    with EnMasseStash(stashobjdict={'test': TestStashObj(foo='bar'),
                                    }) as testinst:
        testinst.stash({'test': 'foo'})


def test_Gula_basic_with_TestStashObj():
    testinst = Gula(test=TestStashObj(foo='bar'))
    testinst.test({'test': 'foo'})
    testinst.close()


def test_Gula_basic_with_TestStashObj_as_context_mnger():
    with Gula(test=TestStashObj(foo='bar')) as testinst:
        testinst.test({'test': 'foo'})


if __name__ == '__main__':
    logging.basicConfig(format=LOGFMT,
                        level=logging.DEBUG,
                        stream=sys.stdout)
    logging.root.level = logging.DEBUG
    logging.basicConfig
    test_EnMasseStash_basic_with_TestStashObj()
    test_EnMasseStash_basic_with_TestStashObj_as_context_mnger()
