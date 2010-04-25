"""Base classes for Node support"""

import itertools
from holland_restore.tokenizer import Tokenizer, RULES
from holland_restore.tokenizer import read_until, yield_until


class Node(object):
    """A collection of tokens representing a logical section
    in a mysqldump file
    """

    def __init__(self, tokens=()):
        self.tokens = list(tokens)
        self.type = None

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


class IterableNode(Node):
    """A node that may not keep all its tokens in a materialized 
    sequence.

    Implements `clear` to ensure the iterator is always exhausted.
    """

    def clear(self):
        """Ensure that tokens iterator is always exhausted to
        ensure we progress to the appropriate point in the backing
        tokenizer
        """
        for token in self.tokens:
            del token


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

    def next_chunk(self, token):
        """Process the token stream given the current token and yield a chunk
        of text

        :param token: decision token
        :type token: `sqlparse.token.Token`
        """
        if token.symbol is 'CreateDatabase':
            block = Node()
            block.type = 'database-ddl'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment'], self._tokenizer))
        elif token.symbol in ('DropTable', 'CreateTable'):
            block = Node()
            block.type = 'table-ddl'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment'], self._tokenizer))
        elif token.symbol in ('LockTable', 'AlterTable', 'InsertRow'):
            # Must use an IterableNode to enable a useful
            # clear() method to exhaust all the symbols in 
            # the node
            block = IterableNode()
            block.type = 'table-dml'
            block.tokens = itertools.chain(self._queue.flush(),
                                yield_until(['SqlComment'], self._tokenizer))
        elif token.symbol is 'ChangeMaster':
            block = Node()
            block.type = 'replication'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment'], self._tokenizer))
        elif token.symbol is 'CreateRoutine':
            block = Node()
            block.type = 'database-routines'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment'], self._tokenizer))
        elif token.symbol is 'CreateTmpView':
            block = Node()
            block.type = 'view-temp-ddl'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment'], self._tokenizer))
        elif token.symbol is 'UseDatabase':
            block = Node()
            block.type = 'view-finalize-db'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment', 
                                            'ConditionalComment'], 
                                           self._tokenizer))
        elif token.symbol in ('DropTmpView'):
            block = Node()
            block.type = 'view-ddl'
            block.tokens.extend(self._queue.flush())
            block.tokens.extend(read_until(['SqlComment'], self._tokenizer))
        else:
            raise ValueError("Can't handle %r queue=%r" % 
                            (token, [t.text for t in self._queue]))
        yield block
        block.clear()

    def iter_chunks(self):
        """Iterate over chunks from the token stream, yielding 
        grouped tokens as Node instances
        """
        
        # symbols that we don't really look at but want to queue until
        # we hit a decision token.
        soft_symbols = ('SqlComment', 
                        'ConditionalComment', 
                        'BlankLine', 
                        'SetVariable', 
                        'DropTable', 
                        'DropView')
        for token in self._tokenizer:
            self._queue.add(token)
            if token.symbol not in soft_symbols:
                for chunk in self.next_chunk(token):
                    if chunk is not None:
                        yield chunk
        final_block = Node()
        final_block.type = 'final'
        final_block.tokens.extend(self._queue.flush())
        yield final_block

    def __iter__(self):
        header = Node()
        header.type = 'dump-header'
        header.tokens.extend(read_until(['BlankLine'], 
                                        self._tokenizer,
                                        inclusive=True))
        yield header
        save_session = Node()
        save_session.type = 'setup-session'
        save_session.tokens.extend(read_until(['BlankLine'], 
                                              self._tokenizer, 
                                              inclusive=True))
        yield save_session
        for chunk in self.iter_chunks():
            yield chunk
