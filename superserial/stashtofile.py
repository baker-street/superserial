# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)


from os import getenv
from functools import partial
from collections import Iterable
from copy import copy

from pathlib import Path
from six.moves.urllib.parse import urlsplit
import boto3
from botocore.client import Config

from superserial.outsidemodules.threading_easy import threading_easy
from superserial.outsidemodules.parallel_easy import map_easy

from superserial.utils import(pass_through,
                              encrypt_it,
                              get_default_data_key,
                              flatten_array_like_strct_gen)
from superserial.outsidemodules.smartopen import ParseUri

import logging
LOG = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Local File System
class LocalFileStash(object):  # Add support for tar archives.
    """
    Used for streaming multiple files to the local file system.

    parenturi - is the directory where the files will be stored.

    The close method has no effect, it's only for creating a consistent API
        with all of the Stash context managers.
    """
    def __init__(self, parenturi, encrypt=False, removeExtIfEncrypt=True,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
                 **xargs):
        self.parentpath = Path(urlsplit(parenturi).path)
        self.encrypt = encrypt
        self.removeExtIfEncrypt = removeExtIfEncrypt
        if not self.parentpath.is_dir():
            self.parentpath.mkdir(parents=True)
        if encrypt:
            self.envelope = partial(encrypt_it, key=encryptkey)
        else:
            self.envelope = pass_through

    def stash(self, datumdict):
        if self.encrypt and self.removeExtIfEncrypt:
            pointer = datumdict['pointer'].split('.')[0]
        else:
            pointer = datumdict['pointer']

        with open(pointer.encode('utf-8'), 'w+') as fp:
            fp.write(self.envelope(datumdict['content']))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


# -----------------------------------------------------------------------------
# S3
def s3_stash_object(bucket, key, body, client, acl='private', encrypt=True, **xargs):
    """
    if encrypt is true (default) then s3 will use server side aes256 encryption,
    which shouldn't cause a noticeable difference in use of the objects stored.
    (assuming they are intended to be non-public)

    additional args to checkout:
        Metadata, ContentEncoding, ServerSideEncryption, SSEKMSKeyId
    Also:
        passing:
            kmsencrypt=True, kmskeyid='a kms key id'
        is equivalent to:
            ServerSideEncryption='aws:kms', SSEKMSKeyId='a kms key id'


    passing 'serversideencryption' and 'ssekmskeyid' will override the default
    encryption method and use the one specified.
    """
    LOG.info('s3_stash_object\tkey:\t' + str(key))
    s3args = dict()
    try:
        s3args['ServerSideEncryption'] = xargs['ServerSideEncryption']
    except KeyError:
        if xargs['kmsencrypt']:
            s3args['ServerSideEncryption'] = 'aws:kms'
        elif encrypt:
            s3args['ServerSideEncryption'] = 'AES256'
    try:
        s3args['SSEKMSKeyId'] = xargs['SSEKMSKeyId']
    except KeyError:
        if xargs['kmsencrypt']:
            s3args['SSEKMSKeyId'] = xargs['kmskeyid']
    # client = boto3.client('s3', config=Config(signature_version='s3v4'))
    return client.put_object(ACL=acl,
                             Bucket=bucket,
                             Key=key,
                             Body=body,
                             **s3args)


class S3FileStash(object):
    """
    parenturi - is the bucket (and 'folder')  where the files will be stored.


    if encrypt is true (default) then s3 will use server side aes256 encryption,
    which shouldn't cause a noticeable difference in use of the objects stored.
    (assuming they are intended to be non-public)

    additional args to checkout:
        metadata, contentencoding, serversideencryption, ssekmskeyid

    passing 'serversideencryption' and 'ssekmskeyid' will override the default
    encryption method and use the one specified.
    """
    def __init__(self, parenturi, encrypt=False, removeExtIfEncrypt=True,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
                 **otherArgsForS3):
        self.parentpath = Path(urlsplit(parenturi).path)
        self.encrypt = encrypt
        self.removeExtIfEncrypt = removeExtIfEncrypt
        self.parsedParentUri = ParseUri(parenturi)
        self.client = boto3.client('s3', config=Config(signature_version='s3v4'))
        self.post_it = partial(s3_stash_object,
                               client=self.client,
                               bucket=str(self.parsedParentUri.bucket_id),
                               encrypt=encrypt,
                               **otherArgsForS3)
        if encrypt:
            self.envelope = partial(encrypt_it, key=encryptkey)
        else:
            self.envelope = pass_through

    def stash(self, datumdict):
        pointer = ParseUri(datumdict['pointer']).key_id.encode('utf-8')
        if self.encrypt and self.removeExtIfEncrypt:
            pointer = pointer.split('.')[0]
        self.post_it(key=pointer,
                     body=self.envelope(datumdict['content']))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def _s3_stash_object_kv_spltr(keyvalue, **xargs):
    if (len(keyvalue) > 2) and isinstance(keyvalue, Iterable):
        return (_s3_stash_object_kv_spltr(keyvalue=kv, **xargs) for kv in keyvalue)
    else:
        output = s3_stash_object(key=keyvalue[0], body=keyvalue[1], **xargs)
        if output:
            return output
        else:
            return 'Done:\t' + keyvalue[0]


def s3_stash_objects_threaded(keyvalueiter, bucket, threads=5,
                              acl='private', encrypt=True, **xargs):

    client = boto3.client('s3', config=Config(signature_version='s3v4'))
    s3_stash_thrdd_prtl = partial(_s3_stash_object_kv_spltr, bucket=bucket, client=client,
                                  acl=acl, encrypt=encrypt, **xargs)
    LOG.info('\t'.join(['keyvalueiter size:', str(len(keyvalueiter)),
                        'keyvalueiter type:', str(type(keyvalueiter))]))
    kvgen = (i for i in keyvalueiter)
    return [output for output in flatten_array_like_strct_gen(threading_easy(s3_stash_thrdd_prtl,
                                                                             kvgen,
                                                                             n_threads=threads))]


def _s3_stash_objects_parallel(keyvalueiter, bucket, vcores=1, threads=5,
                               acl='private', encrypt=True, **xargs):
    s3_stash_thrdd_prtl = partial(s3_stash_objects_threaded, bucket=bucket, threads=threads,
                                  acl=acl, encrypt=encrypt, **xargs)
    LOG.info('\t'.join(['keyvalueiter size:', str(len(keyvalueiter)),
                        'keyvalueiter type:', str(type(keyvalueiter))]))
    for output in flatten_array_like_strct_gen(map_easy(s3_stash_thrdd_prtl,
                                                        list(keyvalueiter),
                                                        n_jobs=vcores)):
        yield output


class S3FileStashPool(object):
    """
    parenturi - is the bucket (and 'folder')  where the files will be stored.


    if encrypt is true (default) then s3 will use server side aes256 encryption,
    which shouldn't cause a noticeable difference in use of the objects stored.
    (assuming they are intended to be non-public)

    additional args to checkout:
        metadata, contentencoding, serversideencryption, ssekmskeyid

    passing 'serversideencryption' and 'ssekmskeyid' will override the default
    encryption method and use the one specified.
    """
    def __init__(self, parenturi, encrypt=False, removeExtIfEncrypt=True,
                 encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
                 vcores=1, threads=50, batch=200,
                 **otherArgsForS3):
        self.vcores = vcores
        self.threads = threads
        self.batch = batch
        self.parentpath = Path(urlsplit(parenturi).path)
        self.encrypt = encrypt
        self.removeExtIfEncrypt = removeExtIfEncrypt
        self.parsedParentUri = ParseUri(parenturi)
        self.post_it = partial(_s3_stash_objects_parallel,
                               vcores=vcores,
                               threads=threads,
                               bucket=str(self.parsedParentUri.bucket_id),
                               encrypt=encrypt,
                               **otherArgsForS3)

        if encrypt:
            self.envelope = partial(encrypt_it, key=encryptkey)
        else:
            self.envelope = pass_through

        self.innerstack = []
        self.outerstack = []

    def _place_on_outerstack(self, stack):
        LOG.info('Stack type:\t' + str(type(stack)))
        self.outerstack.append(stack)
        if self.outerstack >= self.vcores:
            LOG.info('\t'.join(['outerstack size:', str(len(self.outerstack)),
                                'outerstack type:', str(type(self.outerstack))]))
            for output in flatten_array_like_strct_gen(self.post_it(tuple(self.outerstack))):
                LOG.debug(str(output))
            # del(self.outerstack)
            self.outerstack = []

    def _place_on_innerstack(self, obj):
        LOG.info('Obj type:\t' + str(type(obj)))
        self.innerstack.append(obj)
        if len(self.innerstack) >= self.batch:
            LOG.info('\t'.join(['innerstack size:', str(len(self.innerstack)),
                                'innerstack type:', str(type(self.innerstack))]))
            self._place_on_outerstack(stack=tuple(self.innerstack))
            # del(self.innerstack)
            self.innerstack = []

    def stash(self, datumdict):
        pointer = ParseUri(datumdict['pointer']).key_id.encode('utf-8')
        if self.encrypt and self.removeExtIfEncrypt:
            pointer = pointer.split('.')[0]
        self._place_on_innerstack(obj=(pointer,
                                       self.envelope(datumdict['content'])))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


# -----------------------------------------------------------------------------
# All Locations
def file_stash(parenturi, encrypt=False, pool=False,
               encryptkey=getenv('DAS_ENCRYPT_KEY', get_default_data_key()),
               **xargs):
    """
    parenturi - is the directory or bucket (and 'folder')
                where the files will be stored.

    Returns the appropriate obj for stashing based on the parenturi.
    """
    parseduri = ParseUri(parenturi)
    if parseduri.scheme in {'file'}:
        return LocalFileStash(parenturi, encrypt=encrypt, encryptkey=encryptkey,
                              **xargs)
    elif parseduri.scheme in {'s3', 's3n'}:
        if pool:
            return S3FileStashPool(parenturi=parenturi, encrypt=encrypt, encryptkey=encryptkey,
                                   **xargs)
        else:
            return S3FileStash(parenturi, encrypt=encrypt, encryptkey=encryptkey,
                               **xargs)
