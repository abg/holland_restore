"""Base classes for Node support"""

import itertools
from holland_restore.tokenizer import Tokenizer, RULES
from holland_restore.tokenizer import read_until, yield_until
from holland_restore.tokenizer.util import read_sequence
from node_types import *

class TokenQueue(list):
    """Queue for tokens with convenience methods for
    flushing and clearing the queue.
    """

    add = list.append

    def clear(self):
        """Clear the queue"""
        del self[:]

    def flush(self):
        """Flush the queue

        Flushing returns the previous queue and also calls queue.clear()
        """
        try:
            return list(self)
        finally:
            self.clear()

def categorize_comment_block(tokens):
    """Generate a node based only on a set of tokens"""
    meat = tokens[1]
    if 'routines' in meat.text:
        node = DatabaseRoutines(tokens)
    elif 'events' in meat.text:
        node = DatabaseEvents(tokens)
    else:
        raise ValueError("Could not categorize comment: %s", meat.text)
    return node


class NodeStream(object):
    """Process tokens from a mysqldump output tokenizer and
    generate a Node grouping related tokens
    """

    def __init__(self, stream):
        """Create a new DumpParser

        :param stream: stream to parser
        :type stream: any iterable that yields lines for mysqldump output
        """
        self._queue = TokenQueue()
        self._tokenizer = Tokenizer(stream, RULES)
        self._current_db = None

    def process_comments(self, token, tokenizer):
        """Process a comment block.  If it is an empty 'section', try to figure 
        out the node type based on the comment text.
        """
        tokens = read_sequence(['SqlComment', 'SqlComment'], tokenizer)
        next_token = tokenizer.peek()
        if next_token.symbol is 'BlankLine':
            tokens.insert(0, token)
            tokens.append(tokenizer.next())
            if tokenizer.peek().symbol is 'SqlComment':
                # empty section
                return categorize_comment_block(tokens)
        self._queue.extend(tokens)
        return None

    def next_chunk(self, token):
        """Process the token stream given the current token and yield a chunk
        of text

        :param token: decision token
        :type token: `sqlparse.token.Token`
        """
        dispatch = {
            'SetVariable' : self.handle_variable,
            'SqlComment' : self.handle_comment,
            'ConditionalComment' : self.handle_conditional_comment,
            'CreateDatabase' : self.handle_create_db,
            'DropTable' : self.handle_table_ddl,
            'CreateTable' : self.handle_table_ddl,
            'LockTable' : self.handle_table_data,
            'AlterTable' : self.handle_table_data,
            'InsertRow' : self.handle_table_data,
            'ChangeMaster' : self.handle_replication,
            'CreateRoutine' : self.handle_routines,
            'CreateTmpView' : self.handle_temp_view,
            'UseDatabase' : self.handle_reconnect_for_views,
            'DropTmpView' : self.handle_view_ddl,
        }

        try:
            handler = dispatch[token.symbol]
        except KeyError:
            raise ValueError("Can't handle %r[%s] queue=%r" % 
                            (token, token.text, ['%r[%s]' % (t, t.text) 
                                                 for t in self._queue]))
        yield handler(token)

    def handle_variable(self, token):
        assert 'TIME_ZONE' in token.text
        self._queue.append(token)
        for _token in self._tokenizer:
            if _token.symbol in ('SetVariable', 'BlankLine'):
                self._queue.append(_token)
            else:
                self._tokenizer.push_back(_token)
                break
        return RestoreSessionNode(self._queue.flush())

    def handle_comment(self, token):
        try:
            return self.process_comments(token, self._tokenizer)
        except StopIteration:
            return FinalNode([token])

    def handle_conditional_comment(self, token):
        # queue up until we hit something that is != SetVariable
        self._queue.append(token)
        for _token in self._tokenizer:
            if _token.symbol == 'SetVariable':
                self._queue.append(_token)
            else:
                self._tokenizer.push_back(_token)
                break
        
    def handle_create_db(self, token):
        tokens = (self._queue.flush() + [token] +
                  read_until(['SqlComment'], self._tokenizer))

        foo = DatabaseDDL(tokens)
        self._current_db = foo.database
        return foo

    def handle_table_ddl(self, token):
        if self._tokenizer.peek().symbol == 'DropView':
            return self.handle_temp_view(token)
        foo = TableDDL(self._queue.flush() + 
                        [token] +
                        read_until(['SqlComment'], self._tokenizer))
        foo.database = self._current_db
        return foo

    # token.symbol in ('LockTable', 'AlterTable', 'InsertRow'):
    def handle_table_data(self, token):
        tokens = itertools.chain(self._queue.flush() + [token],
                                 yield_until(['SqlComment'], 
                                             self._tokenizer))
        foo = TableDML(tokens)
        foo.database = self._current_db
        return foo

    #elif token.symbol is 'ChangeMaster':
    def handle_replication(self, token):
        tokens = (self._queue.flush() +
                  [self._tokenizer.next()] # blank line
                 )
        return ReplicationNode(tokens)

    #elif token.symbol is 'CreateRoutine':
    def handle_routines(self, token):
        tokens = (self._queue.flush() +
                  read_until(['SqlComment'], self._tokenizer))
        return DatabaseRoutines(tokens)

    #elif token.symbol is 'CreateTmpView':
    def handle_temp_view(self, token):
        tokens = (self._queue.flush() +
                  [token] +
                  read_until(['SqlComment'], self._tokenizer))
        foo = ViewTemporaryDDL(tokens)
        foo.database = self._current_db
        return foo

    #elif token.symbol is 'UseDatabase':
    def handle_reconnect_for_views(self, token):
        tokens = (self._queue.flush() +
                  [token]
                 )
        if self._tokenizer.peek().symbol == 'BlankLine':
            tokens.extend(read_until(['SqlComment', 'ConditionalComment'],
                                     self._tokenizer))
        return ReconnectDBFinalizeView(tokens)

    #elif token.symbol in ('DropTmpView'):
    def handle_view_ddl(self, token):
        tokens = (self._queue.flush() +
                  [token] + 
                  read_until(['SqlComment'], self._tokenizer))
        foo = ViewDDL(tokens)
        foo.database = self._current_db
        return foo
    

    def iter_chunks(self):
        """Iterate over chunks from the token stream, yielding 
        grouped tokens as Node instances
        """
        
        for token in self._tokenizer:
            for chunk in self.next_chunk(token):
                if chunk is not None:
                    yield chunk

    def __iter__(self):
        tokens = read_until(['BlankLine'], 
                            self._tokenizer,
                            inclusive=True)
        foo = HeaderNode(tokens)
        if foo.database:
            self._current_db = foo.database
        yield foo

        tokens = read_until(['BlankLine'], 
                            self._tokenizer, 
                            inclusive=True)
        yield SetupSessionNode(tokens)

        for chunk in self.iter_chunks():
            yield chunk
