#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'superserial'
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__created_on__ = '6/29/2015'

import os
from os import path
from os import getenv
from uuid import uuid4
from random import sample

import pathlib
from arrow import now

from ellis_island.utils import misc
from ellis_island import fullprep as fprep
# from ellis_island import stashenmasse
from ellis_island import stashtodatabase as sdbase
from ellis_island.utils.misc import get_default_data_key
from ellis_island.utils.smartopen import prefix_path_from_uri, ParseUri


from superserial import SQLStash, file_stash, Gula
from superserial.stashtofile import S3FileStashPool

# Imports for script portion.
import click


import logging
LOG = logging.getLogger(__name__)
LOGFMT = '%(levelname)s\tproc:%(process)d thread:%(thread)d module:%(module)s\
\t%(message)s'


PROJECT = unicode(uuid4()).split('-')[0]

TESTHELP = '''If test mode,the meta table will end with the projects name.
'''


@click.command()
@click.argument('inputdir', default='/mnt/input')
# help='Directory to pull files from. Full path.')
@click.option('--meta',
              default='sqlite:////mnt/output/metadata.db',
              help='Where to store the captured metadata. A uri.')
@click.option('--datafile', default='/mnt/output/textdata/',
              help='Where to store the out put data files. A uri.')
@click.option('-n', default=0, type=int,
              help='Number of files to use. If 0, it will use all found files.')
@click.option('--randsmpl/--no-randsmpl', default=True,
              help="If True, pick a random sub sample.")
@click.option('-c', default=3,
              help='The number of parallel processes. If 1, will run linear.')
@click.option('--project', default=PROJECT,
              help='The project id to use.')
@click.option('--metatable', default='metadata',
              help='The table to store the metadata in.')
@click.option('--test/--no-test', default=False,
              help=TESTHELP)
@click.option('--encrypt/--no-encrypt', default=False,
              help="Encrypt the files? (does not include metadata")
@click.option('--encryptkey', default=getenv('DAS_ENCRYPT_KEY',
                                             get_default_data_key()),
              help='The encryption key to use')
@click.option('--kmsencrypt/--no-kmsencrypt', default=False,
              help="Use kms to encrypt files going to s3.")
@click.option('--kmskeyid', default='',
              help='The key id of the kms encryption key to use. For s3.')
@click.option('--dontstop/--no-dontstop', default=False,
              help="Skips docs that raise errors.")
@click.option('--log', default='/mnt/logs/ellis_island.log',
              help='The full filename for the log file.')
@click.option('-ioc', default=1, type=int,
              help='The number of parallel IO processes. If 1, will run linear.')
@click.option('-iot', default=1, type=int,
              help='The number of IO threads. If 1, no threads will be used.')
@click.option('-iob', default=200, type=int,
              help='The IO batch size.')
def main(inputdir,
         meta,
         datafile,
         n,
         randsmpl,
         c,
         project,
         metatable,
         test,
         encrypt,
         encryptkey,
         kmsencrypt,
         kmskeyid,
         dontstop,
         log,
         ioc,
         iot,
         iob):
    starttime = now()
    count = n  # TODO (steven_c) clean this up
    vcores = c
    os.environ['CURRENT_PROJECT_UUID'] = project
    if test:
        project = project + PROJECT
        metatable = metatable + '_' + project
        logfname = log + '.' + project
        datafile = path.join(datafile, project)
    else:
        logfname = log
    # --------
    # logging
    logging.basicConfig(format=LOGFMT,
                        level=logging.DEBUG,
                        filename=logfname)
    logging.root.level = logging.DEBUG
    logging.basicConfig
    # --------
    try:
        encryptkey = encryptkey.strip()
    except AttributeError:
        LOG.debug('No encryptkey was passed passed or found.')
    LOG.info('\t'.join(['Project:', project, 'Table:', metatable]))
    # --
    # IO
    emaillist = list(misc.spelunker_gen(inputdir))
    # --
    # Proc
    if not count:
        emlsmpl = emaillist
    elif randsmpl:
        emlsmpl = sample(emaillist, count)
    else:
        emlsmpl = emaillist[0:count]
    LOG.info('Population size:\t' + str(len(emaillist)))
    LOG.info('Sample size:\t' + str(len(emlsmpl)))
    batchsize = 50
    prefix = prefix_path_from_uri(datafile)
    respackiter = fprep.clean_and_register_en_masse(emlsmpl,
                                                    prefix=prefix,
                                                    dontstop=dontstop,
                                                    njobs=vcores,
                                                    batchsize=batchsize,
                                                    ordered=False,
                                                    project=project)
    """
    def clean_pack(packiter):
        for pack in packiter:
            if not pack['meta']['use']:
                pack.pop('text')
            # pack['meta'].pop('extra')
            yield pack

    newpackiter = clean_pack(respackiter)
    """
    # --
    # IO
    outroottext = path.join(datafile, 'text/')
    outrootraw = path.join(datafile, 'raw/')
    if ParseUri(datafile).scheme in {'file'}:
        outroottextobj = pathlib.Path(outroottext)
        if not outroottextobj.is_dir():
            outroottextobj.mkdir(parents=True)
        outrootrawobj = pathlib.Path(outrootraw)
        if not outrootrawobj.is_dir():
            outrootrawobj.mkdir(parents=True)
    # dbcon = 'postgresql://tester:test12@localhost:2345/docmeta'

    iogenargs = {'encrypt': encrypt,
                 'encryptkey': encryptkey,
                 'kmsencrypt': kmsencrypt,
                 'kmskeyid': kmskeyid,
                 }
    if iot > 1:
        iogenargs.update({'pool': True,
                          'threads': iot,
                          'vcores': ioc,
                          'batch': iob,
                          })
    # sdbase.default_create_table_sqlalchemy(meta, tablename=metatable)
    stashobjdict = {  # 'meta': SQLStash(uri=meta, table=metatable),
                    'text': file_stash(parenturi=outroottext,
                                       **iogenargs),
                    'raw': file_stash(parenturi=outrootraw,
                                      **iogenargs),
                    }
    chomp = Gula(**stashobjdict)

    def rm_meta(rpackiter):
        for pack in rpackiter:
            pack.pop('meta')
            yield pack

    rpckitr = rm_meta(respackiter)
    chomp.consume(rpckitr)
    LOG.info('\t'.join(['VCores:',
                        str(vcores),
                        ]))
    LOG.info('\t'.join(['BatchSize:',
                        str(batchsize),
                        ]))
    endtime = now()
    LOG.info('\t'.join(['RunTime:',
                        str(endtime - starttime),
                        ]))


if __name__ == '__main__':
    main()
