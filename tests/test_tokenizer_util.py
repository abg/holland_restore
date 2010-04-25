from nose.tools import *
from holland_restore.tokenizer import Tokenizer
from holland_restore.tokenizer.rules import RULES
from holland_restore.tokenizer.util import *
import textwrap

def test_yield_until():
    text = textwrap.dedent("""
    -- MySQL dump 10.13  Distrib 5.1.42, for redhat-linux-gnu (x86_64)
    --
    -- Host: localhost    Database: sakila
    -- ------------------------------------------------------
    -- Server version       5.1.42-rs-log

    /*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
    """).lstrip()

    tokenizer = Tokenizer(text.splitlines(True), RULES)
    
    for token in yield_until(['BlankLine'], tokenizer, inclusive=False):
        assert_equals(token.symbol, 'SqlComment')

    tokenizer = Tokenizer(text.splitlines(True), RULES)
    for token in yield_until(['BlankLine'], tokenizer, inclusive=True):
        if token.symbol == 'BlankLine':
            break
        assert_equals(token.symbol, 'SqlComment')
    assert_equals(token.symbol, 'BlankLine')

def test_read_until():
    text = textwrap.dedent("""
    -- MySQL dump 10.13  Distrib 5.1.42, for redhat-linux-gnu (x86_64)
    --
    -- Host: localhost    Database: sakila
    -- ------------------------------------------------------
    -- Server version       5.1.42-rs-log

    /*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
    """).lstrip()

    tokenizer = Tokenizer(text.splitlines(True), RULES)
    
    for token in read_until(['BlankLine'], tokenizer, inclusive=False):
        assert_equals(token.symbol, 'SqlComment')

def test_yield_until_preserving():
    text = textwrap.dedent("""
    --
    -- Table structure for table `actor`
    --
    
    DROP TABLE IF EXISTS `actor`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!40101 SET character_set_client = utf8 */;
    CREATE TABLE `actor` (
        `actor_id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
        `first_name` varchar(45) NOT NULL,
        `last_name` varchar(45) NOT NULL,
        `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`actor_id`),
        KEY `idx_actor_last_name` (`last_name`)
    ) ENGINE=InnoDB AUTO_INCREMENT=201 DEFAULT CHARSET=utf8;
    /*!40101 SET character_set_client = @saved_cs_client */;
    
    --
    -- Dumping data for table `actor`
    --

    LOCK TABLES `actor` WRITE;
    /*!40000 ALTER TABLE `actor` DISABLE KEYS */;
    INSERT INTO `actor` VALUES (1,'PENELOPE','GUINESS','2006-02-15 10:34:33')
    """).lstrip()

    tokenizer = Tokenizer(text.splitlines(True), RULES)
    scan_until_preserving(['LockTable'], tokenizer, preserve_symbols=['BlankLine', 'SqlComment'])   
    assert_equals(tokenizer.next().symbol, 'BlankLine')
    ok_(tokenizer.next().text.startswith('--'))
    ok_(tokenizer.next().text.startswith('-- Dumping data for table `actor`'))
    assert_equals(tokenizer.next().symbol, 'SqlComment')
    assert_equals(tokenizer.next().symbol, 'BlankLine')
    assert_equals(tokenizer.next().symbol, 'LockTable')
