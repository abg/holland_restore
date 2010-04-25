from nose.tools import *
from holland_restore.tokenizer import Token
from holland_restore.tokenizer import Tokenizer, TokenizationError

def test_token():
    token = Token('Test', 'Table `foo bar`', (1,1), -1)

    assert_equals(token.line_range, (1,1))
    assert_equals(token.offset, -1)
    ok_(token.symbol is 'Test')
    assert_equals(token.text, 'Table `foo bar`')

    ok_(token.match_prefix('Table'), 'Prefix match failed')
    ok_(token.match_regex('.*?`[^`]+`'), 'Regular expression match failed')
    name, = token.extract('`([^`]+)`')
    assert_equals(name, 'foo bar')
    result = token.extract('Fail')
    ok_(result is None)

    assert_equals(repr(token), 'Token(symbol=\'Test\')')

def test_tokenizer():
    tokenizer = Tokenizer([], ())
    assert_raises(StopIteration, tokenizer.next)

    # empty rule set
    tokenizer = Tokenizer(['Foo'], ())
    assert_raises(TokenizationError, tokenizer.next)
    try:
        tokenizer = Tokenizer(['Foo'], ())
        tokenizer.next()
    except TokenizationError, exc:
        assert_equals(str(exc), 'No tokenization rule matched.')

def test_tokenizer_pushback():
    tokenizer = Tokenizer(['Foo'], [lambda x, y: Token('sample', x, (), -1)])
    token1 = tokenizer.next()
    tokenizer.push_back(token1)
    token2 = tokenizer.next()
    assert_equals(token1, token2)


def test_tokenizer_iteration():
    tokenizer = Tokenizer(['Foo'], [lambda x, y: Token('sample', x, (), -1)])

    for token in tokenizer:
        pass
    assert_equals(token.text, 'Foo')

import holland_restore.tokenizer.rules as token_rules
from holland_restore.scanner import Scanner
import textwrap
from StringIO import StringIO

def test_make_token():
    stream = textwrap.dedent("""
    -- Host: localhost    Database: sakila
    -- Dumping data for table `actor`
    """).splitlines()
    scanner = Scanner(stream)
    line = scanner.next()
    token = token_rules.make_token('SqlComment', line, scanner)

    assert_equals(token.text, line)
    assert_equals(token.symbol, 'SqlComment')
    assert_equals(token.offset, 0)

def test_tokenizer_prefix():
    """Test that we tokenize a line that matches a prefix"""
    stream = textwrap.dedent("""\
    -- Dumping data for table `actor`
    """).splitlines()
    scanner = Scanner(stream)

    tokenize_prefix = token_rules.tokenize_prefix
    make_token = token_rules.make_token
    tokenizer = tokenize_prefix('-- Dumping data', 
                                make_token, 
                                'TableData')
    token = tokenizer(scanner.next(), scanner)
    assert_equals(token.symbol, 'TableData')

def test_tokenize_blank():
    tokenize_blank = token_rules.tokenize_blank
    scanner = Scanner("\n")
    token = tokenize_blank(scanner.next(), scanner)
    assert_equals(token.symbol, 'BlankLine')
    scanner = Scanner(["foo\n"])
    token = tokenize_blank(scanner.next(), scanner)
    ok_(token is None, "Got token %r, expected None" % token)

def test_tokenize_multiline():
    text = textwrap.dedent("""
    CREATE TABLE `actor` (
        `actor_id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
        `first_name` varchar(45) NOT NULL,
        `last_name` varchar(45) NOT NULL,
        `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`actor_id`),
        KEY `idx_actor_last_name` (`last_name`)
    ) ENGINE=InnoDB AUTO_INCREMENT=201 DEFAULT CHARSET=utf8;
    """).lstrip()

    tokenize_multi_line = token_rules.tokenize_multi_line
    scanner = Scanner(StringIO(text))
    token = tokenize_multi_line('CreateTable', ';', scanner.next(), scanner)
    assert_equals(token.symbol, 'CreateTable')
    assert_equals(token.line_range, (1, text.count("\n")))
    ok_(token.text.startswith('CREATE TABLE `actor`'))
    ok_(token.text.rstrip().endswith(';'))

def test_tokenize_stored_procedure():
    text = textwrap.dedent("""
    DELIMITER ;;
    /*!50003 CREATE*/ /*!50020 DEFINER=`root`@`localhost`*/ /*!50003 PROCEDURE `film_in_stock`(IN p_film_id INT, IN p_store_id INT, OUT p_film_count INT)
        READS SQL DATA
    BEGIN
        SELECT inventory_id
        FROM inventory
        WHERE film_id = p_film_id
        AND store_id = p_store_id
        AND inventory_in_stock(inventory_id);

        SELECT FOUND_ROWS() INTO p_film_count;
    END */;;
    DELIMITER ;
    """).lstrip()

    scanner = Scanner(StringIO(text))
    tokenize_delimiter = token_rules.tokenize_delimiter
    token = tokenize_delimiter(scanner.next(),
                               scanner)
    assert_equals(token.symbol, 'CreateRoutine')
    assert_equals(token.text, text)

def test_tokenize_trigger():
    text = textwrap.dedent("""
    DELIMITER ;;
    /*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `customer_create_date` BEFORE INSERT ON `customer` FOR EACH ROW SET NEW.create_date = NOW() */;;
    DELIMITER ;
    """).lstrip()

    scanner = Scanner(StringIO(text))
    token = token_rules.tokenize_delimiter(scanner.next(), scanner)
    assert_equals(token.symbol, 'CreateTrigger')
    assert_equals(token.text, text)

def test_tokenize_conditional():
    text = textwrap.dedent("""
    /*!40000 ALTER TABLE `actor` DISABLE KEYS */;
    /*!50001 DROP TABLE IF EXISTS `actor_info`*/;
    /*!50001 DROP VIEW IF EXISTS `film_list`*/;
    /*!50001 CREATE TABLE `actor_info` (
        `actor_id` smallint(5) unsigned,
        `first_name` varchar(45),
        `last_name` varchar(45),
        `film_info` varchar(341)
    ) ENGINE=MyISAM */;
    /*!50001 CREATE ALGORITHM=UNDEFINED */
    /*!50013 DEFINER=`root`@`localhost` SQL SECURITY INVOKER */
    /*!50001 VIEW `actor_info` AS select `a`.`actor_id` AS `actor_id`,`a`.`first_name` AS `first_name`,`a`.`last_name` AS `last_name`,group_concat(distinct concat(`c`.`name`,_utf8': ' ,(select group_concat(`f`.`title` order by `f`.`title` ASC separator ', ') AS `GROUP_CONCAT(f.title ORDER BY f.title SEPARATOR ', ')` from ((`film` `f` join `film_category` `fc` on((`f`.`film_id` = `fc`.`film_id`))) join `film_actor` `fa` on((`f`.`film_id` = `fa`.`film_id`))) where ((`fc`.`category_id` = `c`.`category_id`) and (`fa`.`actor_id` = `a`.`actor_id`)))) order by `c`.`name` ASC separator '; ') AS `film_info` from (((`actor` `a` left join `film_actor` `fa` on((`a`.`actor_id` = `fa`.`actor_id`))) left join `film_category` ` fc` on((`fa`.`film_id` = `fc`.`film_id`))) left join `category` `c` on((`fc`.`category_id` = `c`.`category_id`))) group by `a`.`actor_id`,`a`.`first_name`,`a`.`last_name` */;
    /*!50001 SET character_set_client      = @saved_cs_client */;
    /*!30223 START SLAVE */;
    """).lstrip()

    scanner = Scanner(StringIO(text))
    tokenizer = token_rules.distinguish_conditional
    expect = [
        'AlterTable',
        'DropTmpView',
        'DropView',
        'CreateTmpView',
        'CreateView',
        'SetVariable',
        'ConditionalComment',
    ]

    for run, line in enumerate(scanner):
        token = tokenizer(line, scanner)
        assert_equals(token.symbol, expect[run])

def test_tokenize_sql_comment():
    text = textwrap.dedent("""
    --
    -- Position to start replication or point-in-time recovery from
    --
    -- CHANGE MASTER TO MASTER_LOG_FILE='bin-log.000007', MASTER_LOG_POS=296;
    """).lstrip()

    scanner = Scanner(StringIO(text))

    expected_symbols = [
        'SqlComment',
        'SqlComment',
        'SqlComment',
        'ChangeMaster',
    ]

    for run, line in enumerate(scanner):
        token = token_rules.distinguish_sql_comment(line, scanner)
        assert_equals(token.symbol, expected_symbols[run])
