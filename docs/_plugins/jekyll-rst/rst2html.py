#!/usr/bin/env python

# :Author: David Goodger, the Pygments team, Guenter Milde
# :Date: $Date: $
# :Copyright: This module has been placed in the public domain.

# This is a merge of the `Docutils`_ `rst2html` front end with an extension
# suggestion taken from the `Pygments`_ documentation, reworked specifically
# for `Octopress`_.
#
# .. _Pygments: http://pygments.org/
# .. _Docutils: http://docutils.sourceforge.net/
# .. _Octopress: http://octopress.org/

"""
A front end to docutils, producing HTML with syntax colouring using pygments
"""

try:
    import locale
    locale.setlocale(locale.LC_ALL, '')
except:
    pass

from transform import transform
from docutils.writers.html4css1 import Writer
from docutils.core import default_description
from directives import Pygments

description = ('Generates (X)HTML documents from standalone reStructuredText '
               'sources. Uses `pygments` to colorize the content of'
               '"code-block" directives. Needs an adapted stylesheet' 
               + default_description)

def main():
    return transform(writer=Writer(), part='html_body')

if __name__ == '__main__':
    print(main())
