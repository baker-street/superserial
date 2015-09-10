# -*- coding: utf-8 -*-
__author__ = 'Steven Cutting'
__author_email__ = 'steven.c.projects@gmail.com'
__created_on__ = '7/13/2015'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"

from . import(__title__, __version__, __status__, __license__, __maintainer__,
              __email__)


from os import getenv
from functools import partial

from pathlib import Path
from six.moves.urllib.parse import urlsplit
import boto3
from botocore.client import Config

from superserial.utils import pass_through, encrypt_it
from superserial.utils import get_default_data_key
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
def s3_stash_object(bucket, key, body, acl='private', encrypt=True, **xargs):
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
    client = boto3.client('s3', config=Config(signature_version='s3v4'))
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
        self.post_it = partial(s3_stash_object,
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


# -----------------------------------------------------------------------------
# All Locations
def file_stash(parenturi, encrypt=False,
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
        return S3FileStash(parenturi, encrypt=encrypt, encryptkey=encryptkey,
                           **xargs)
