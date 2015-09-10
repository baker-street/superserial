# -*- coding: utf-8 -*-
"""
Uri parser (ParseUri) adapted from smart_open.

Source location: https://github.com/piskvorky/smart_open

# Copyright (C) 2015 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).

Full smart_open license in: smart_open-LICENSE.txt
"""
import os
from six.moves.urllib.parse import urlsplit

import logging
LOG = logging.getLogger(__name__)


class ParseUri(object):
    """
    Parse the given URI.
    Supported URI schemes are "file", "s3", "s3n" and "hdfs".
    Valid URI examples::
      * s3://my_bucket/my_key
      * s3://my_key:my_secret@my_bucket/my_key
      * hdfs:///path/file
      * ./local/path/file
      * ./local/path/file.gz
      * file:///home/user/file
      * file:///home/user/file.bz2
    """
    def __init__(self, uri, default_scheme="file"):
        """
        Assume `default_scheme` if no scheme given in `uri`.
        """
        if os.name == 'nt':
            # urlsplit doesn't work on Windows --
            # it parses the drive as the scheme...
            if '://' not in uri:
                # no protocol given => assume a local file
                uri = 'file://' + uri
        parsed_uri = urlsplit(uri)
        self.scheme = parsed_uri.scheme if parsed_uri.scheme else default_scheme

        if self.scheme == "hdfs":
            self.uri_path = parsed_uri.netloc + parsed_uri.path

            if not self.uri_path:
                raise RuntimeError("invalid HDFS URI: %s" % uri)
        elif self.scheme in ("s3", "s3n"):
            self.bucket_id = (parsed_uri.netloc + parsed_uri.path).split('@')
            self.key_id = None

            if len(self.bucket_id) == 1:
                # URI without credentials: s3://bucket/object
                self.bucket_id, self.key_id = self.bucket_id[0].split('/', 1)
                # "None" credentials are interpreted as "look for credentials
                # in other locations" by boto
                self.access_id, self.access_secret = None, None
            elif (len(self.bucket_id) == 2 and
                  len(self.bucket_id[0].split(':')) == 2):
                # URI in full format: s3://key:secret@bucket/object
                # access key id: [A-Z0-9]{20}
                # secret access key: [A-Za-z0-9/+=]{40}
                acc, self.bucket_id = self.bucket_id
                self.access_id, self.access_secret = acc.split(':')
                self.bucket_id, self.key_id = self.bucket_id.split('/', 1)
            else:
                # more than 1 '@' means invalid uri
                # Bucket names must be at least 3 and no more than 63 characters
                # long.
                # Bucket names must be a series of one or more labels.
                # Adjacent labels are separated by a single period (.).
                # Bucket names can contain lowercase letters, numbers, and
                # hyphens.
                # Each label must start and end with a lowercase letter or a
                # number.
                raise RuntimeError("invalid S3 URI: %s" % uri)
        elif self.scheme == 'file':
            self.uri_path = parsed_uri.netloc + parsed_uri.path

            if not self.uri_path:
                raise RuntimeError("invalid file URI: %s" % uri)
        else:
            raise NotImplementedError("unknown URI scheme %r in %r" %
                                      (self.scheme, uri))


def prefix_path_from_uri(uri):
    parseduri = ParseUri(uri)
    if parseduri.scheme in {'file'}:
        return parseduri.uri_path
    elif parseduri.scheme in {'s3', 's3n'}:
        return ''.join([parseduri.scheme, '://',
                        parseduri.bucket_id, '/',
                        parseduri.key_id,
                        ])
    errmsg = ''.join(['URI of type: ', parseduri.scheme,
                      ' is not yet handled by this function. ',
                      prefix_path_from_uri.__module__, '.',
                      prefix_path_from_uri.__name__,
                      '\tThe full URI: ', uri,
                      ])
    LOG.critical(errmsg)
    raise ValueError(errmsg)
