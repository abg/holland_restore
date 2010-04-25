"""Utility methods for working with `sqlparse.lex.Lexer` token streams"""
def read_until(stop_symbols, tokenizer, inclusive=False):
    """Read tokens from tokenizer until we hit a symbol in ``symbols``
    and return the text of all tokens read up to or including that point

    :param symbols: list of `Symbol` items
    :type symbols: list
    :param tokenizer: `Lexer`
    :param inclusive: bool. whether to include the stop token in output
    :returns: str. concatenated text of all tokens
    """
    return [token for token in yield_until(stop_symbols, tokenizer, inclusive)]

def yield_until(stop_symbols, tokenizer, inclusive=False):
    """Read tokens from tokenizer until we hit a symbol in ``symbols``
    and yield each token in turn, also including the stop token if 
    ``inclusive`` is not False.

    Unlike ``read_until`` this will return a generator, yielding a single
    token's text at a time.  When tokens may include large chunks of text
    this can be more memory efficient than read_until

    :param symbols: stop list of token symbols.  If a token's symbol is in this
                        list we will stop read any further tokens.
    :param tokenizer: `Lexer` token stream to read tokens from
    :param inclusive: boolean flag indicating whether we should include the stop token
                      as an output of this function
    """
    for tok in tokenizer:
        if tok.symbol in stop_symbols:
            if inclusive is False:
                tokenizer.push_back(tok)
            else:
                yield tok
            break
        yield tok

def yield_until_preserving(stop_symbols, 
                           tokenizer, 
                           inclusive=False, 
                           preserve_symbols=()):
    """Yield tokens from the tokenizer until a symbol in `symbols` is
    encountered.
    
    While scanning the tokenizer, this method will queue up any tokens in 
    `wsymbols`.  If a token is encountered that is not in `wsymbols` and is
    also not in `symbols` the queue is flushed.
 
    This is largely used to scan through the tokenizer but not skip over some
    preceding tokens such as a comment block.
    """
    queue = []
    for token in yield_until(stop_symbols, tokenizer, inclusive):
        if token.symbol in preserve_symbols:
            queue.append(token)
        else:
            while queue:
                yield queue.pop(0)
            yield token
    # ensure proper ordering of tokenizer symbols
    # if inclusive and we hit a stop symbol ``symbols``
    # then it was pushed on the stack - it should come after
    # anything we have in 'queue' as those token precede this
    # token
    if not inclusive:
        token = tokenizer.next()
        if token:
            queue.append(token)
    while queue:
        token = queue.pop(0)
        tokenizer.push_back(token)


def scan_until_preserving(stop_symbols, tokenizer, preserve_symbols=()):
    """Loop through the tokenizer discarding all symbols until a token with a
    matching symbol in `symbols` is encountered. 
    
    Like `yield_until_windowed` this method will also queue any symbols founds 
    in queue_symbols while scanning
    """
    for token in yield_until_preserving(stop_symbols,
                                        tokenizer,
                                        preserve_symbols=preserve_symbols):
        token = token
