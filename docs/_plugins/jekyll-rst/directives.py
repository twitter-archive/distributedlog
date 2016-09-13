# Define a new directive `code-block` (aliased as `sourcecode`) that uses the 
# `pygments` source highlighter to render code in color. 
#
# Incorporates code from the `Pygments`_ documentation for `Using Pygments in 
# ReST documents`_ and `Octopress`_.
#
# .. _Pygments: http://pygments.org/
# .. _Using Pygments in ReST documents: http://pygments.org/docs/rstdirective/
# .. _Octopress: http://octopress.org/

import re
import os
import hashlib
import __main__

# Absolute path to pygments cache dir
PYGMENTS_CACHE_DIR = os.path.abspath(os.path.join(os.path.dirname(__main__.__file__), '../../.pygments-cache'))

# Ensure cache dir exists
if not os.path.exists(PYGMENTS_CACHE_DIR):
    os.makedirs(PYGMENTS_CACHE_DIR)

from pygments.formatters import HtmlFormatter

from docutils import nodes
from docutils.parsers.rst import directives, Directive

from pygments import highlight
from pygments.lexers import get_lexer_by_name, TextLexer

class Pygments(Directive):
    """ Source code syntax hightlighting.
    """
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = True
    string_opts = ['title', 'url', 'caption']
    option_spec = dict([(key, directives.unchanged) for key in string_opts])
    has_content = True

    def run(self):
        self.assert_has_content()
        try:
            lexer_name = self.arguments[0]
            lexer = get_lexer_by_name(lexer_name)
        except ValueError:
            # no lexer found - use the text one instead of an exception
            lexer_name = 'text'
            lexer = TextLexer()
        formatter = HtmlFormatter()

        # Construct cache filename
        cache_file = None
        content_text = u'\n'.join(self.content)
        cache_file_name = '%s-%s.html' % (lexer_name, hashlib.md5(content_text).hexdigest())
        cached_path = os.path.join(PYGMENTS_CACHE_DIR, cache_file_name)

        # Look for cached version, otherwise parse
        if os.path.exists(cached_path):
            cache_file = open(cached_path, 'r')
            parsed = cache_file.read()
        else:
            parsed = highlight(content_text, lexer, formatter)

        # Strip pre tag and everything outside it
        pres = re.compile("<pre>(.+)<\/pre>", re.S)
        stripped = pres.search(parsed).group(1)

        # Create tabular code with line numbers
        table = '<div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers">'
        lined = ''
        for idx, line in enumerate(stripped.splitlines(True)):
            table += '<span class="line-number">%d</span>\n' % (idx + 1)
            lined  += '<span class="line">%s</span>' % line
        table += '</pre></td><td class="code"><pre><code class="%s">%s</code></pre></td></tr></table></div>' % (lexer_name, lined)

        # Add wrapper with optional caption and link
        code = '<figure class="code">'
        if self.options:
            caption = ('<span>%s</span>' % self.options['caption']) if 'caption' in self.options else ''
            title = self.options['title'] if 'title' in self.options else 'link'
            link = ('<a href="%s">%s</a>' % (self.options['url'], title)) if 'url' in self.options else ''

            if caption or link:
                code += '<figcaption>%s %s</figcaption>' % (caption, link)
        code += '%s</figure>' % table

        # Write cache
        if cache_file is None:
            cache_file = open(cached_path, 'w')
            cache_file.write(parsed)
        cache_file.close()

        return [nodes.raw('', code, format='html')]

directives.register_directive('code-block', Pygments)
directives.register_directive('sourcecode', Pygments)