"""Tokenizer base classes"""

import re
from holland_restore.scanner import Scanner

__all__ = [
    'Token',
    'TokenizationError',
    'Tokenizer',
]

class Token(object):
    """Lex Token"""
    __slots__ = ('symbol', 'text', 'line_range', 'offset')

    def __init__(self, symbol, text, line_range, offset):
        """Create a new token

        :param symbol: The token symbol
        :type symbol: str
        :param text: The token text
        :type text: str
        :param line_range: start and end lines of the token in the source data
        :type line_range: tuple
        :param offset: byte offset of the token text within the source data
        :type offset: int
        """
        self.symbol = symbol
        self.text = text
        self.line_range = line_range
        self.offset = offset

    def match_prefix(self, prefix):
        """Check if this token starts with the given prefix
        
        :param prefix: text prefix to match against this token
        :type prefix: str

        :returns: bool. True if this token's text starts with the prefix and
                        False otherwise
        """
        return self.text.startswith(prefix)

    def match_regex(self, regex):
        """Check if this token matches the given regular expresssion string
        
        :param regex: regular expression to match this token's text against
        :type regex: str
        :returns: bool. True if this token's text matches the regular 
                        expression and False otherwise.
        """
        return re.match(regex, self.text) is not None

    def extract(self, regex):
        """Search this token for the specific regex
        
        :param regex: regular expression to search for in this token's text
        :type regex: str
        :returns: iterable of the matching groups
        """
        match = re.search(regex, self.text, re.M|re.U)
        if not match:
            return None
        return match.groups()

    def __str__(self): 
        return "Token(symbol=%r)" % (self.symbol)

    def __repr__(self):
        return str(self)

class TokenizationError(Exception):
    """Raised when a problem tokenizing text is encountered"""
    def __init__(self, message, line, position):
        Exception.__init__(self, message)
        self.message = message
        self.line = line
        self.position = position

    def __str__(self):
        return self.message


class Tokenizer(object):
    """A simple line-based tokenizer"""

    def __init__(self, stream, rules=()):
        """Create a new Tokenizer

        :param stream: stream to read tokens from
        :type stream: iterable
        :param rules: tokenization rules that takes data from the
                      stream and produces tokens
        :type rules: iterable of callables. 
                     Each rule callable should accept two parameters:
                        * line - current text being considered
                        * scanner - the internal tokenizer scanner. A subclass
                                    of `Scanner`
        """
        self.scanner = Scanner(stream)
        self.rules = list(rules)
        self.token_queue = []

    def push_back(self, token):
        """Place the given token at the front of the Tokenizer's queue

        :param token: token to push back
        :type token: `Token`
        """       
        self.token_queue.append(token)

    def peek(self):
        """Peek at the next token in the tokenizer but don't move 
        past it.

        This is equivalent to calling next() and pushing the token
        back on the tokenizer stack.

        :returns: `Token`
        """
        token = self.next()
        self.push_back(token)
        return token

    def next(self):
        """Return the next available token.

        If tokens have been pushed back on the stream
        those are returned before we pull more input from
        the scanner.
        """
        if self.token_queue:
            return self.token_queue.pop(0)
        else:
            return self.tokenize()

    def tokenize(self):
        """Generate a token based on the next line in the scanner"""
        scanner = self.scanner
        line = scanner.next()
        for rule in self.rules:
            token = rule(line, self.scanner)
            if token is not None:
                return token
        raise TokenizationError("No tokenization rule matched.",
                                line, scanner.position)

    def __iter__(self):
        return self
