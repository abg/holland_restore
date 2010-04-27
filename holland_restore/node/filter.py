"""Node Filtering"""

def emit_node(node):
    """Yield the concatenated text of all tokens in a node.

    This will materialize an entire node
    """
    yield str(node)

def stream_node(node):
    """Stream text from a node.

    Unlike emit_node this will go in smaller chunks intentionally to avoid 
    materializing a very large node
    """
    for token in node.tokens:
        yield str(token.text)

class NodeFilter(object):
    """Filter a Node from a NodeStream with a simple
    dispatcher.
    """

    def __init__(self):
        # map node types to some action which can
        # be used for filtering
        self._dispatch = {
            'dump-header' : emit_node,
            'setup-session' : emit_node,
            'restore-session' : emit_node,
            'replication' : emit_node,
            'final' : emit_node,
            'database-ddl' : emit_node,
            'table-ddl' : emit_node,
            'table-dml' : stream_node,
            # initial create table for view
            'view-temp-ddl' : emit_node,
            # final drop table/create view to really
            # create view
            'view-ddl' : emit_node,
            'view-finalize-db' : emit_node,
            'database-routines' : emit_node,
            'database-events' : emit_node,
        }
        self._rewriters = {}
        self.register('dump-header', parse_header)
        self.register('database-ddl', parse_database)
        self.register('table-ddl', parse_table)
        self.register('view-temp-ddl', parse_view)
        self.register('view-ddl', parse_view)

    def register(self, event, callback):
        """Register a node handler"""
        try:
            events = self._rewriters[event]
        except KeyError:
            events = self._rewriters.setdefault(event, [])

        events.append(callback)
    
    def unregister(self, event, callback):
        """Unregister a node handler"""
        try:
            self._rewriters[event].remove(callback)
        except KeyError:
            pass

    def __call__(self, node):
        try:
            dispatch = self._dispatch[node.type]
        except KeyError:
            raise LookupError("No handler for %r" % node)
        # rewrite a node 
        try:
            for callback in self._rewriters[node.type]:
                node = callback(self, node)
        except KeyError:
            # no rewriter for node type
            pass
        except:
            node.clear()
            raise
        return dispatch(node)

def parse_database(self, node):
    for token in node:
        if token.symbol in ('UseDatabase', 'CreateDatabase'):
            self.database, = token.extract('.*?`((?:``|[^`])+)`')
            break
    return node

def parse_table(self, node):
    for token in node:
        if token.symbol in ('CreateTable',):
            self.table, = token.extract('.*?`((?:``|[^`])+)`')
            break
    return node

def parse_view(self, node):
    for token in node:
        if token.symbol == 'CreateTmpView':
            self.table, = token.extract('.*?`((?:``|[^`])+)`')
            break
        if token.symbol == 'CreateView':
            self.table, = token.extract('^/[*]!50001 VIEW `((?:``|[^`])+)`')
            break
    return node

def parse_header(self, node):
    for token in node:
        if 'Database: ' in token.text:
            self.database, = token.extract('.*Database: (.*)$')
    return node
