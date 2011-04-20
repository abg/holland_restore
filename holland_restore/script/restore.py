"""Command-line front-end to mysqldump output parsing"""
import sys
import time
from optparse import OptionParser
from holland_restore.node import NodeStream, NodeFilter
from holland_restore.node.util import skip_databases, skip_tables, \
                                      skip_engines, skip_node, \
                                      skip_triggers, skip_binlog, \
                                      SkipNode

def build_opt_parser():
    """Build an OptionParser"""
    opt_parser = OptionParser()
    opt_parser.add_option('--toc',
                          action='store_true',
                          help="Show table of contents for the specified dump"
                               "files.",
                          default=False)
    opt_parser.add_option('--table', '-t', 
                          metavar="db.tbl",
                          dest='tables',
                          action='append',
                          help=("Only include tables with the specified name. "
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
    if opts.tables != ['*'] or opts.exclude_tables:
        for tbl in opts.tables:
            if '.' in tbl:
                db, name = tbl.split('.')
                if db not in opts.databases:
                    print >>sys.stderr, "Note: Adding implicit database inclusion '%s' from --table %s" % (db, tbl)
                    opts.databases.remove('*')
                    opts.databases.append(db)
        for tbl in opts.exclude_tables:
            if '.' in tbl:
                db, name = tbl.split('.')
                if db not in opts.exclude_databases:
                    print >>sys.stderr, "Note: Adding implicit database exclusion '%s' from --exclude-table %s" % (db, tbl)
                    opts.databases.remove('*')
                    opts.exclude_databases.append(db)
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
       
import signal

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

    if opts.toc:
        return cmd_toc(args)

    process(node_filter, args)
    return 0

from threading import Thread
import time

class SimpleWrapper(object):
    def __init__(self, stream):
        self.stream = stream
        self.count = 0

    def next(self):
        line = self.stream.next()
        self.count += len(line)
        return line

    def __iter__(self):
        return self

    def __getattr__(self, key):
        return getattr(self.stream, key)

def process(node_filter, args):
    if not args:
        args = '-'

    for arg in args:
        if arg == '-':
            fileobj = sys.stdin
        else:
            fileobj = open(arg, 'r')
        try:
            fileobj = SimpleWrapper(fileobj)
            fancy_bar = ProgressBar('green', width=40)
            start = time.time()
            data = Data()
            monitor = ProgressMonitor(fancy_bar, data=data)
            stream_filter(node_filter, fileobj, monitor)
        finally:
            monitor.stop()

from util import ProgressMonitor
from progress import ProgressBar
from threading import Lock
import itertools

class Data(object):
    def __init__(self):
        self.state = 'Initializing'
        self.position = (0, 0)
        self.start = time.time()
        self.percent = itertools.cycle(xrange(101))
        self.lock = Lock()

    def update(self, state, position):
        self.lock.acquire()
        self.state = state
        self.lock.release()

    def poll(self):
        self.lock.acquire()
        line, offset = self.position.position
        percent = self.percent.next()
        total = (offset / 1024.0**2)
        rate = total / (time.time() - self.start)
        message = "\n".join([
            "",
            "Processing: %s" % self.state,
            "Line: %d" % line,
            "%.2f MB (%.2f MB per second)" % (total, rate),
            "Elapsed: %.2f seconds" % (time.time() - self.start),
        ])
        self.lock.release()
        return percent, message

def stream_filter(node_filter, fileobj, monitor, stream=sys.stdout):
    state = 'initializing'
    node_stream = NodeStream(fileobj)
    if monitor:
        monitor.data.position = node_stream._tokenizer.scanner
    try:
        if monitor:
            monitor.start()
        for node in node_stream:
            state = format_node(node)
            if monitor:
                monitor.data.update(state, node_stream._tokenizer.scanner.position)
            stream_node(node_filter, node, stream)
    finally:
        state = 'Interrupted'
        if monitor:
            monitor.stop()
    if monitor:
        monitor.join()
    if sys.exc_info() != (None, None, None):
        raise

def stream_node(node_filter, node, stream):
    try:
        for chunk in node_filter(node):
            stream.write(str(chunk))
    except SkipNode:
        print >>sys.stderr, "skipping node %r" % node
        pass

def progress_info(input, position, state, start):
    return int(input.count / (250.0*1024**2)), "\n".join([
        'Processing: %s' % state,
        'Line: %d' % position[0],
        'Data Read: %.2f MB' % (input.count / 1024.0**2),
        'Read rate: %.2f MB per second' % ((input.count / (time.time() - start)) / 1024.0**2),
        'Data Written: %.2f MB' % (position[1] / 1024.0**2),
        'Time Elapsed: %.2f seconds' % (time.time() - start),
    ])

def cmd_toc(args):
    if not args:
        args = '-'

    for arg in args:
        if arg == '-':
            fileobj = sys.stdin
        else:
            fileobj = open(arg, 'r')
        table_of_contents(fileobj)

    return 0

def table_of_contents(fileobj):
    node_stream = NodeStream(fileobj)
    print fileobj.name
    print "="*len(fileobj.name)
    database = ''
    table = ''
    for node in node_stream:
        try:
            database = node.database
        except AttributeError:
            node.database = database

        first_token = iter(node.tokens).next()
        for last_token in node.tokens:
            pass
        start_byte = first_token.offset
        end_byte = last_token.offset + len(last_token.text)
        start_line = first_token.line_range[0]
        end_line = last_token.line_range[1]
        print "%-20s %-40s bytes:%-20s lines:%-10s" % \
            (node.type, format_node(node),
             "%d-%d" % (start_byte, end_byte), 
             "%d-%d" % (start_line, end_line))

def format_node(node):
    if node.type == 'database-ddl':
        return "`%s` (database)" % node.database
    elif node.type == 'table-ddl':
        return "`%s`.`%s` (ddl)" % (node.database, node.table)
    elif node.type == 'table-dml':
        return "`%s`.`%s` (data)" % (node.database, node.table)
    elif node.type == 'view-temp-ddl':
        return "`%s`.`%s` (view [temp])" % (node.database, node.table)
    elif node.type == 'view-ddl':
        return '`%s`.`%s` (view)' % (node.database, node.table)
    elif node.type == 'database-routines':
        return '`%s` (routines)' % (node.database)
    elif node.type == 'view-finalize-db':
        return '`%s` (databasex2)' % (node.database)
    return node.type

def file_size(fileobj):
    import os
    import stat
    st = os.fstat(fileobj.fileno())
    if stat.S_ISREG(st.st_mode):
        return st.st_size
    else:
        return None

