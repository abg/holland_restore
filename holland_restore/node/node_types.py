"""Node types emitted by a NodeStream"""

import itertools

class Node(object):
    """A collection of tokens representing a logical section
    in a mysqldump file
    """

    def __init__(self, tokens=()):
        self.tokens = tokens

    def __str__(self):
        return "".join([t.text for t in self.tokens])

    def __repr__(self):
        return self.__class__.__name__ + "(type=%r)" % self.type

    def __iter__(self):
        return iter(self.tokens)

    def find(self, symbol):
        """Find a token with the given symbol in this node

        :returns: `Token`
        :raises: `LookupError`
        """
        for token in self.tokens:
            if token.symbol is symbol:
                return token
        raise LookupError("No token found for symbol %r" % symbol)

    def clear(self):
        """Clear any state saved by this token

        This must be overriden in a subclass to be useful
        as the default implementation does nothing
        """

class ReplicationNode(Node):
    """Representation of a node containing replication status

    CHANGE MASTER TO ...
    """
    type = 'replication'

    def position(self):
        """A convenience function to extract out the position in replication"""
        for token in self.tokens:
            if token.symbol == 'ChangeMaster':
                binlog, position = token.extract("'([^']+).*(\d+)'")
                position = int(position)
                return binlog, position
    position = property(position)

class HeaderNode(Node):
    """Representation of the initial comment block in a mysqldump
    file.
    """
    type = 'dump-header'

    def database(self):
        """In a --databases dump, the header will also list the name of 
        the database
        """
        for token in self.tokens:
            if 'Database:' in token.text:
                return token.extract("Database: (.*)$")[0]
    database = property(database)

class DatabaseDDL(Node):
    """Representation of a node containing DDL to create/connect to a 
    database
    """
    type = 'database-ddl'

    def database(self):
        for token in self.tokens:
            if '`' in token.text:
                return token.extract('`((?:``|[^`])+)`')[0]
    database = property(database)

class SetupSessionNode(Node):
    """Representation of a node containing a set of SET VARIABLE statements
    used to save the state of the existing session before proceeding with
    a dump
    """
    type = 'setup-session'

class RestoreSessionNode(Node):
    """Representation of a node containing a set of SET VARIABLE statemetns
    to restore the previous state of a session before a dump started
    """
    type = 'restore-session'

class ViewDDL(Node):
    """Node containing CREATE VIEW DDL"""
    type = 'view-ddl'
 
    def table(self):
        tokens = []
        for token in self.tokens:
            tokens.append(token)
            if '`' in token.text:
                return token.extract('`((?:``|[^`])+)`')[0]
    table = property(table)   

class ViewTemporaryDDL(ViewDDL):
    type = 'view-temp-ddl'

class TableDDL(Node):
    """Node containing CREATE TABLE DDL"""
    type = 'table-ddl'

    def table(self):
        tokens = []
        for token in self.tokens:
            tokens.append(token)
            if '`' in token.text:
                return token.extract('`((?:``|[^`])+)`')[0]
    table = property(table)

class TableDML(Node):
    """Node containing table data"""
    type = 'table-dml'
    
    def table(self):
        return 'dml'
    table = property(table)

    def __str__(self):
        return 'TableDML()'

    def clear(self):
        """Ensure that tokens iterator is always exhausted to
        ensure we progress to the appropriate point in the backing
        tokenizer
        """
        for token in self.tokens:
            del token

class DatabaseRoutines(Node):
    """Node containing routines in a database"""
    type = 'database-routines'

    def database(self):
        for token in self.tokens:
            if "'" in token.text:
                return token.extract("'((?:``|[^`])+)'")[0]
    database = property(database)

    def routines(self):
        for token in self.tokens:
            if 'FUNCTION `' in token.text:
                yield token.extract('FUNCTION `((?:``|[^`])+)`')[0]
            if 'PROCEDURE `' in token.text:
                yield token.extract('PROCEDURE `((?:``|[^`])+)`')[0]
    routines = property(routines)

class DatabaseEvents(Node):
    """Node containing database events"""
    type = 'database-events'

class ReconnectDBFinalizeView(Node):
    """Node containing a USE statement before restoring views for a schema"""
    type = 'view-finalize-db'
 
    def database(self):
        for token in self.tokens:
            if '`' in token.text:
                return token.extract('`((?:``|[^`])+)`')[0]
    database = property(database)

class FinalNode(Node):
    """Node containing the final Dump Completed line"""
    type = 'final'
