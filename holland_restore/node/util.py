"""Node utility methods"""

import logging
from holland_restore.tokenizer import Token
from holland_restore.node.base import SkipNode
from holland_restore.util import Filter, FilteredItem

class SkipNode(Exception):
    """Raised when a node is skipped by a utility filter"""

def skip_databases(include=('*',), exclude=()):
    """Create a handler to evaluate database-ddl and skip the node based
    on an inclusion/exclusion check

    :param include: list of glob patterns that must match for inclusions
    :param exclude: list of glob patterns that should be excluded
    """
    check = Filter()
    check.include(list(include))
    check.exclude(list(exclude))
    def _skip_handler(dispatcher, node):
        """Process a node and skip based on database filter"""
        try:
            check(dispatcher.database)
        except FilteredItem:
            raise SkipNode()
        return node
    return _skip_handler

def skip_tables(include=('*',), exclude=()):
    """Create a handler to evaluate table-* nodes and optionally skip these 
    based on an inclusion/exclusion check

    :param include: list of glob patterns that must match for inclusions
    :param exclude: list of glob patterns that should be excluded
    """
    check = Filter()
    check.include(list(include))
    check.exclude(list(exclude))
    def _skip_handler(dispatcher, node):
        """Process a node and skip based on table filter"""
        try:
            check('%s.%s' % (dispatcher.database, dispatcher.table))
        except FilteredItem:
            raise SkipNode()
        return node
    return _skip_handler

def skip_engines(include=('*',), exclude=()):
    """Create a handler to evaluate table-ddl nodes and skip
    ddl + dml based on a table engine using an inclusion/exclusion
    check

    :param include: list of glob patterns that must match for inclusions
    :param exclude: list of glob patterns that should be excluded
    """
    check = Filter()
    check.include([pat.lower() for pat in include])
    check.exclude([pat.lower() for pat in exclude])

    def _skip_handler(dispatcher, node):
        """Process node and skip based on engine filters"""
        for token in node.tokens:
            if token.symbol is 'CreateTable':
                engine, = token.extract('^[)] ENGINE=([a-zA-Z]+)')
            elif token.symbol is 'CreateTmpView':
                engine = 'view'
            else:
                continue
            try:
                check(engine.lower())
            except FilteredItem:
                name = '%s.%s' % (dispatcher.database, dispatcher.table)
                # add a check for the dm - should be a one shot check
                if engine == 'view':
                    dispatcher.register('view-ddl', skip_tables(exclude=[name]))
                else:
                    dispatcher.register('table-dml', 
                                        skip_tables(exclude=[name]))
                raise SkipNode()
        return node
    return _skip_handler

def skip_node(dispatcher, node):
    """Unconditionally skip a node

    :param dispatcher: dispatcher instance that is dispatching to us
    :param node: Node node that is being considered
    """
    logging.debug("skip_node(dispatcher=%r, node=%r)", dispatcher, node)
    raise SkipNode("Skipping Node", node)

def skip_binlog(dispatcher, node):
    """Rewrite a Node to disable binary logging for the session

    :param dispatcher: dispatcher instance that is dispatching to us
    :param node: Node node that is being considered
    """
    lines = "\n".join([
        '/*!40101 SET @OLD_SQL_LOG_BIN=@@SQL_LOG_BIN */;',
        '/*!40101 SET SQL_LOG_BIN = 0 */;',
        ''
    ])
    node.tokens.insert(-1, Token('NoBinLog', lines, (), -1))
    return node

def skip_triggers(dispatcher, node):
    """Skip triggers in a table-dml section

    :param dispatcher: dispatcher instance that is dispatching to us
    :param node: Node node that is being considered
    """
    def filter_triggers(tokens):
        for token in tokens:
            if token.symbol in ('CreateTrigger', 'SetVariable'):
                continue
            yield token
            if token.symbol == 'BlankLine':
                break
    node.tokens = filter_triggers(node.tokens)
    return node
