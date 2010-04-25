"""Command-line front-end to mysqldump output parsing"""
import sys
from optparse import OptionParser
from holland_restore.node import NodeStream, NodeFilter
from holland_restore.node.util import skip_databases, skip_tables, \
                                      skip_engines, skip_node, skip_binlog, \
                                      SkipNode

def build_opt_parser():
    """Build an OptionParser"""
    opt_parser = OptionParser()
    opt_parser.add_option('--tables', '-t', action='append', default=[])
    opt_parser.add_option('--exclude-tables', '-T', 
                          action='append',
                          default=[])
    opt_parser.add_option('--databases', '-d', action='append', default=[])
    opt_parser.add_option('--exclude-databases', '-D', 
                          action='append',
                          default=[])
    opt_parser.add_option('--engines', '-e', action='append', default=[])
    opt_parser.add_option('--exclude-engines', '-E',
                          action='append',
                          default=[])
    opt_parser.add_option('--no-data', action='store_true', default=False)
    opt_parser.add_option('--skip-binlog', action='store_true', default=False)
    opt_parser.add_option('--skip-triggers',
                          action='store_true',
                          default=False)
    opt_parser.add_option('--skip-routines',
                          action='store_true',
                          default=False)
    return opt_parser

def setup_misc_filters(opts, node_filter):
    """Add misc filters to the node_filter based on requested options"""
    if opts.no_data:
        node_filter.register('table-dml', skip_node)
    if opts.skip_binlog:
        node_filter.register('setup-session', skip_binlog)

    if opts.skip_routines:
        node_filter.register('database-routines', skip_node)

def setup_database_filters(opts, node_filter):
    """Add database filters to the node_filter based on requested options"""
    if opts.databases != ['*'] or opts.exclude_databases:
        skip_handler = skip_databases(include=opts.databases,
                                      exclude=opts.exclude_databases)
        node_filter.register('database-ddl', skip_handler)
        node_filter.register('table-ddl', skip_handler)
        node_filter.register('table-dml', skip_handler)
        node_filter.register('database-routines', skip_handler)
        node_filter.register('database-events', skip_handler)

def setup_table_filters(opts, node_filter):
    """Add table filters to the node_filter based on requested options"""
    if opts.tables != ['*'] or opts.exclude_tables:
        skip_handler = skip_tables(include=opts.tables,
                                   exclude=opts.exclude_tables)
        node_filter.register('table-ddl', skip_handler)
        node_filter.register('view-temp-ddl', skip_handler)
        node_filter.register('view-ddl', skip_handler)
        node_filter.register('table-dml', skip_handler)

def setup_engine_filters(opts, node_filter):
    """Add engine filters to the node_filter based on requested options"""
    if opts.engines != ['*'] or opts.exclude_engines:
        skip_handler = skip_engines(include=opts.engines,
                                    exclude=opts.exclude_engines)
        node_filter.register('table-ddl', skip_handler)
        
def main(args=None):
    """Main entry point for CLI frontend"""
    opt_parser = build_opt_parser()
    opts, args = opt_parser.parse_args(args)
    if not opts.tables:
        opts.tables = ['*']
    if not opts.databases:
        opts.databases = ['*']
    if not opts.engines:
        opts.engines = ['*']

    node_filter = NodeFilter()

    setup_misc_filters(opts, node_filter)
    setup_database_filters(opts, node_filter)
    setup_table_filters(opts, node_filter)
    setup_engine_filters(opts, node_filter)

    try:
        process(node_filter, args)
    except IOError, exc:
        return 1
    except KeyboardInterrupt:
        return 1
    return 0

def process(node_filter, args):
    if not args:
        args = '-'

    for arg in args:
        if arg == '-':
            fileobj = sys.stdin
        else:
            fileobj = open(arg, 'r')
        stream_filter(node_filter, fileobj)

def stream_filter(node_filter, fileobj, write_pipe=lambda txt: sys.stdout.write(txt)):
    node_stream = NodeStream(fileobj)
    for node in node_stream:
        try:
            for chunk in node_filter(node):
                write_pipe(str(chunk))
        except SkipNode, exc:
            continue

