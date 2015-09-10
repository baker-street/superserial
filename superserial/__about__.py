# -*- coding: utf-8 -*-
__title__ = 'superserial'
__version__ = '0.1.0'
__status__ = "3 - Alpha"
__author__ = 'Steven Cutting'
__author_email__ = 'steven.e.cutting@linux.com'
__copyright__ = "superserial  Copyright (C) 2015  Steven Cutting"
__license__ = "GPL3"
__created_on__ = '8/30/2015'
__maintainer__ = "Steven Cutting"
__email__ = 'steven.e.cutting@linux.com'

import os.path

__all__ = ["__title__", "__version__", "__status__", "__commit__", "__author__",
           "__author_email__", "__license__", "__copyright__",
           "__maintainer__", "__email__",
           ]
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = None

if base_dir is not None and os.path.exists(os.path.join(base_dir, ".commit")):
    with open(os.path.join(base_dir, ".commit")) as fp:
        __commit__ = fp.read().strip()
else:
    __commit__ = None
