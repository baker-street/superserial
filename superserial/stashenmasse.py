# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/15/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)

import logging
LOG = logging.getLogger(__name__)

from functools import partial
from itertools import chain, izip_longest

from superserial.outsidemodules.parallel_easy import imap_easy


def _io_negotiator_make(stashobjdict, **xargs):
    if type(stashobjdict) != dict:
        raise ValueError('stashobjdict should be a dict')

    def io_negotiator_stash(datumtype, datum, **xargs):
        stashobjdict[datumtype].stash(datum, **xargs)

    def io_negotiator_close(**xargs):
        for stashobjtype, stashobj in stashobjdict.items():
            LOG.debug(str(stashobjtype) + " is now closed.")
            stashobj.close(**xargs)

    return io_negotiator_stash, io_negotiator_close


class EnMasseStash(object):
    def __init__(self, stashobjdict, keystoignore={'id'}, **xargs):
        # TODO (steven_c) consider handling the encrypt key through xargs.
        io_neg_stash, io_neg_close = _io_negotiator_make(stashobjdict,
                                                         **xargs)
        self.io_negotiator_stash = io_neg_stash
        self.io_negotiator_close = io_neg_close
        self.keystoignore = keystoignore

    def stash(self, datumdict):
        for key, value in datumdict.items():
            if key not in self.keystoignore:
                self.io_negotiator_stash(datumtype=key,
                                         datum=value)

    def close(self):
        self.io_negotiator_close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def stash_en_masse(datumiter, stashobjdict,
                   **xargs):
    """
    stashobjdict - dict that holds the stashobjects.
                   the key will be used to guide the packets.
    """
    # TODO (steven_c) consider handling the encrypt key through xargs.
    with EnMasseStash(stashobjdict, **xargs) as stashobj:
        for i, datum in enumerate(datumiter):
            stashobj.stash(datum)
            LOG.info('\t'.join(['DocsIterd:', str(i)]))


def _stash(datum, stashobjdict,
           **xargs):
    """
    stashobjdict - dict that holds the stashobjects.
                   the key will be used to guide the packets.
    """
    with EnMasseStash(stashobjdict, **xargs) as stashobj:
        stashobj.stash(datum)


def kobayashi(datumiter, stashobjdict, njobs=2, chunksize=50, ordered=False,
              **xargs):
    """
    stashobjdict - dict that holds the stashobjects.
                   the key will be used to guide the packets.
    """
    stasher = partial(_stash, stashobjdict=stashobjdict, **xargs)
    out = imap_easy(stasher, datumiter, n_jobs=njobs, chunksize=chunksize,
                    ordered=ordered)
    return out


def _sort_iterables(*iters, **xargs):
    try:
        if xargs['swap']:
            queue = (datumdict
                     for combo in izip_longest(fillvalue=None, *iters)
                     for datumdict in combo if datumdict)
        else:
            raise KeyError
    except KeyError:
        queue = chain(*iters)
    return queue


class Gula(object):
    def __init__(self, **stash_objs):
        io_neg_stash, io_neg_close = _io_negotiator_make(stash_objs)
        self._io_negotiator_stash = io_neg_stash
        self._io_negotiator_close = io_neg_close
        self.keystoignore = {'id'}
        self._repr = 'Gula(' + repr(stash_objs) + ')'

        for k, v in stash_objs.items():
            self.__dict__[k] = v.stash

    def stash(self, datumdict):
        for k, v in datumdict.items():
            if k not in self.keystoignore:
                self._io_negotiator_stash(datumtype=k,
                                          datum=v)

    @staticmethod
    def _sort_iterables(*iters, **xargs):
        try:
            if xargs['swap']:
                queue = (datumdict
                         for combo in izip_longest(fillvalue=None, *iters)
                         for datumdict in combo if datumdict)
            else:
                raise KeyError
        except KeyError:
            queue = chain(*iters)
        return queue

    def consume(self, *datumiter, **xargs):
        queue = self._sort_iterables(*datumiter, **xargs)
        i = 0  # if datumiter is zero len
        for i, datumdict in enumerate(queue):
            self.stash(datumdict=datumdict)
            if not i % 100:
                LOG.info('DatumsIterd:\t' + str(i))
        if len(datumiter):
            LOG.info('DatumsIterd:\t' + str(i))

    def close(self):
        LOG.debug('Gura has now closed')
        self._io_negotiator_close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        LOG.debug('Gura, exit has been called.')
        self.close()

    def __repr__(self):
        return self._repr

    def __str__(self):
        return repr(self)

    def __unicode__(self):
        return unicode(str(self))
