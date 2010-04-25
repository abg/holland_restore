"""Set of rules for tokenizing mysqldump output"""

from cStringIO import StringIO
from holland_restore.tokenizer.base import Token

__all__ = [
    'RULES',
]

def tokenize_prefix(prefix, dispatch, *args, **kwargs):
    """Tokenization rule that tokenizes if a line matches
    a specified prefix.
    """
    def match(line, scanner):
        """Match a line starts with the prefix specified in the wrapper 
        function
        """
        if line.startswith(prefix):
            return dispatch(line=line, scanner=scanner, *args, **kwargs)
    return match

def make_token(symbol, line, scanner):
    """Create a new token instance with the specified token id and
    current scanner context
    """
    return Token(symbol,
                 line,
                 (scanner.lineno, scanner.lineno),
                 scanner.offset)

def tokenize_blank(line, scanner):
    """Tokenization rule that matches if a line is only whitespace.
    """
    # Minimum non-whitespace line will be '--\n'
    if len(line) <= 2:
        return make_token('BlankLine', line, scanner)

def tokenize_multi_line(symbol, until, line, scanner):
    """Tokenize text that spans multiple lines given a prefix
    and suffix marker.
    """
    lineno = scanner.lineno
    offset = scanner.offset
    text = StringIO()
    text.write(line)
    for line in scanner:
        text.write(line)
        if line.rstrip().endswith(until):
            break
    data = text.getvalue()
    del text
    return Token(symbol, 
                 text=data, 
                 line_range=(lineno,scanner.lineno), 
                 offset=offset)

def tokenize_delimiter(symbol, until, line, scanner):
    """Tokenize and classify text between DELIMITER markers"""
    token = tokenize_multi_line(symbol, until, line, scanner)
    if '/*!50003 TRIGGER' in token.text:
        token.symbol = 'CreateTrigger'
    else:
        token.symbol = 'CreateRoutine'

    return token

def distinguish_conditional(line, scanner):
    """Tokenize and classify a MySQL comment line"""
    if line.startswith('/*!40000 ALTER'):
        token = make_token(symbol='AlterTable', line=line, scanner=scanner)
    elif line.startswith('/*!50001 DROP TABLE'):
        token = make_token(symbol='DropTmpView', line=line, scanner=scanner)
    elif line.startswith('/*!50001 DROP VIEW'):
        token = make_token(symbol='DropView', line=line, scanner=scanner)
    elif line.startswith('/*!50001 CREATE TABLE'):
        token = tokenize_multi_line(symbol='CreateTmpView', 
                                    until=';', 
                                    line=line, 
                                    scanner=scanner)
    elif line.startswith('/*!50001 CREATE '):
        token = tokenize_multi_line(symbol='CreateView',
                                    until=';', 
                                    line=line, 
                                    scanner=scanner)
    elif line.lstrip('/*!0123456789 ').startswith('SET '):
        token = make_token(symbol='SetVariable', line=line, scanner=scanner)
    else:
        token = make_token(symbol='ConditionalComment',
                           line=line,
                          scanner=scanner)
    return token

def distinguish_sql_comment(line, scanner):
    """Tokenize and classify a SQL comment line

    In MySQL, CHANGE MASTER may be commented out - we still classify this
    as a 'ChangeMaster' token.
    """
    if line.startswith('-- CHANGE MASTER'):
        symbol = 'ChangeMaster'
    else:
        symbol = 'SqlComment'
    return make_token(symbol, line, scanner)

tokenize_change_master = tokenize_prefix('CHANGE MASTER',
                                         make_token,
                                         symbol='ChangeMaster')

tokenize_set_variable = tokenize_prefix('SET ', 
                                        make_token, 
                                        symbol='SetVariable')

tokenize_comment = tokenize_prefix('--', distinguish_sql_comment)
                                   

tokenize_create_db = tokenize_prefix('CREATE DATABASE', 
                                     make_token, 
                                     symbol='CreateDatabase')

tokenize_use_db = tokenize_prefix('USE ', make_token, symbol='UseDatabase')

tokenize_drop_table = tokenize_prefix('DROP TABLE', 
                                      make_token, 
                                      symbol='DropTable')

tokenize_lock_tables = tokenize_prefix('LOCK ',
                                       make_token, 
                                       symbol='LockTable')

tokenize_unlock_tables = tokenize_prefix('UNLOCK ', 
                                         make_token, 
                                         symbol='UnlockTable')

tokenize_insert = tokenize_prefix('INSERT', 
                                  make_token, 
                                  symbol='InsertRow')

tokenize_replace = tokenize_prefix('REPLACE', 
                                  make_token, 
                                  symbol='ReplaceTable')

tokenize_conditional_comment = tokenize_prefix('/*!', distinguish_conditional)

tokenize_create_table = tokenize_prefix('CREATE TABLE', 
                                        tokenize_multi_line, 
                                        symbol='CreateTable', until=';')

tokenize_delimiter = tokenize_prefix('DELIMITER ;;', 
                                     tokenize_delimiter, 
                                     symbol='Delimiter', 
                                     until='DELIMITER ;')


RULES = [
    tokenize_change_master,
    tokenize_comment,
    tokenize_blank,
    tokenize_conditional_comment,
    tokenize_create_table,
    tokenize_create_db,
    tokenize_use_db,
    tokenize_drop_table,
    tokenize_lock_tables,
    tokenize_unlock_tables,
    tokenize_delimiter,
    tokenize_insert,
    tokenize_replace,
    tokenize_set_variable,
]
