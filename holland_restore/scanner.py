"""A simple line scanner implementation"""

import itertools

class Scanner(object):
    """Read lines from an iterable and track the current byte-offset and 
    line number
    """
    
    def __init__(self, stream):
        """Create a LineScanner for the specified stream

        :param stream: iterable stream to parse lines from
        :type stream:  any iterable that yields lines of text. This can 
                       include file-like objects, lists of strings or other 
                       sources.
        """
        self.stream = iter(stream)
        self.lineno = 0
        self.offset = 0
        self.next_offset = 0

    def __iter__(self):
        """Iterate over this scanner"""
        return self

    def next(self):
        """Return the next line in the scanner's stream"""
        atom = self.stream.next()
        self.offset = self.next_offset
        self.next_offset += len(atom)
        self.lineno += 1
        return atom


    def push_back(self, line):
        """Push a line back into the scanner so it is read again

        :param line: line of text to push back into the scanner
        """
        self.next_offset = self.offset
        self.offset -= len(line)
        self.lineno -= 1
        self.stream = itertools.chain([line], self.stream)

    def position(self):
        """Return the scanner's current line number and position"""
        return self.lineno, self.offset
    position = property(position)
