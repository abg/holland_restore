"""Command-line front-end to mysqldump output parsing"""
import sys
from optparse import OptionParser
from holland_restore.node import NodeStream, NodeFilter
from holland_restore.node.util import skip_databases, skip_tables, \
                                      skip_engines, skip_node, \
                                      skip_triggers, skip_binlog, \
                                      SkipNode

def build_opt_parser():
    """Build an OptionParser"""
    opt_parser = OptionParser()
    opt_parser.add_option('--table', '-t', 
                          metavar="db.tbl",
                          dest='tables',
                          action='append',
                          help=("Only includ tables with the specified name. "
                                "This option may be specified multiple times."
                                ),
                          default=[])
    opt_parser.add_option('--exclude-table', '-T', 
                          metavar="db.tbl",
                          dest='exclude_tables',
                          action='append',
                          help=("Exclude tables and data with the specified "
                                "table name. This option may be specified "
                                "multiple times."),
                          default=[])
    opt_parser.add_option('--database', '-d',
                          metavar="database",
                          action='append',
                          dest='databases',
                          help=("Only include the specified database. This "
                                "option may be specified multiple times."),
                          default=[])
    opt_parser.add_option('--exclude-database', '-D', 
                          metavar="database",
                          action='append',
                          dest='exclude_databases',
                          default=[])
    opt_parser.add_option('--engine', '-e',
                          metavar="engine",
                          action='append', 
                          dest='engines',
                          help=("Only output tables and table data with the "
                                "specified storage engine.  This option may "
                                "be specified multiple times."),
                          default=[])
    opt_parser.add_option('--exclude-engine', '-E',
                          metavar="engine",
                          action='append',
                          dest='exclude_engines',
                          help=("Exclude the specified engine. This option "
                                "may be specified multiple times"),
                          default=[])
    opt_parser.add_option('--no-data', 
                          action='store_true', 
                          help=("Only output database schema, not data."),
                          default=False)
    opt_parser.add_option('--skip-binlog', 
                          action='store_true', 
                          help=("Add SQL_LOG_BIN = 0 to the top of the dump "
                                "and disable logging the import to the "
                                "binary log"),
                          default=False)
    opt_parser.add_option('--skip-triggers',
                          action='store_true',
                          help=("Remove CREATE TRIGGER blocks from the output"),
                          default=False)
    opt_parser.add_option('--skip-routines',
                          action='store_true',
                          help=("Remove functions/stored procedures from the output"),
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
    if opts.skip_triggers:
        node_filter.register('table-dml', skip_triggers)

def setup_database_filters(opts, node_filter):
    """Add database filters to the node_filter based on requested options"""
    if opts.databases != ['*'] or opts.exclude_databases:
        skip_handler = skip_databases(include=opts.databases,
                                      exclude=opts.exclude_databases)
        node_filter.register('database-ddl', skip_handler)
        node_filter.register('view-finalize-db', skip_handler)
        node_filter.register('table-ddl', skip_handler)
        node_filter.register('table-dml', skip_handler)
        node_filter.register('view-temp-ddl', skip_handler)
        node_filter.register('view-ddl', skip_handler)
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
        node_filter.register('view-temp-ddl', skip_handler)
        
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

