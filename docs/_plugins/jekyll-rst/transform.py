import sys
from docutils.core import publish_parts
from optparse import OptionParser
from docutils.frontend import OptionParser as DocutilsOptionParser
from docutils.parsers.rst import Parser

def transform(writer=None, part=None):
    p = OptionParser(add_help_option=False)
    
    # Collect all the command line options
    docutils_parser = DocutilsOptionParser(components=(writer, Parser()))
    for group in docutils_parser.option_groups:
        p.add_option_group(group.title, None).add_options(group.option_list)
    
    p.add_option('--part', default=part)
    
    opts, args = p.parse_args()
    
    settings = dict({
        'file_insertion_enabled': False,
        'raw_enabled': False,
    }, **opts.__dict__)
    
    if len(args) == 1:
        try:
            content = open(args[0], 'r').read()
        except IOError:
            content = args[0]
    else:
        content = sys.stdin.read()
    
    parts = publish_parts(
        source=content,
        settings_overrides=settings,
        writer=writer,
    )
    
    if opts.part in parts:
        return parts[opts.part]
    return ''