from holland_restore.node import NodeStream
from nose.tools import *
import textwrap

def test_header_nodes():
    text = textwrap.dedent("""
    -- MySQL dump 10.13  Distrib 5.1.42, for redhat-linux-gnu (x86_64)
    --
    -- Host: localhost    Database: sakila
    -- ------------------------------------------------------
    -- Server version       5.1.42-rs-log

    /*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
    /*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
    /*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
    /*!40101 SET NAMES utf8 */;
    /*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
    /*!40103 SET TIME_ZONE='+00:00' */;
    /*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
    /*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
    /*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
    /*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

    --
    -- Position to start replication or point-in-time recovery from
    --

    -- CHANGE MASTER TO MASTER_LOG_FILE='bin-log.000007', MASTER_LOG_POS=296;
    
    --
    -- Current Database: `sakila`
    --

    CREATE DATABASE /*!32312 IF NOT EXISTS*/ `sakila` /*!40100 DEFAULT CHARACTER SET latin1 */;

    USE `sakila`;

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
    INSERT INTO `actor` VALUES (1,'PENELOPE','GUINESS','2006-02-15 10:34:33');
    /*!40000 ALTER TABLE `actor` ENABLE KEYS */;
    UNLOCK TABLES;


    --
    -- Temporary table structure for view `actor_info`
    --

    DROP TABLE IF EXISTS `actor_info`;
    /*!50001 DROP VIEW IF EXISTS `actor_info`*/;
    SET @saved_cs_client     = @@character_set_client;
    SET character_set_client = utf8;
    /*!50001 CREATE TABLE `actor_info` (
        `actor_id` smallint(5) unsigned,
        `first_name` varchar(45),
        `last_name` varchar(45),
        `film_info` varchar(341)
    ) ENGINE=MyISAM */;
    SET character_set_client = @saved_cs_client;
    
    --
    -- Dumping routines for database 'sakila'
    --
    /*!50003 DROP PROCEDURE IF EXISTS `film_in_stock` */;
    /*!50003 SET @saved_cs_client      = @@character_set_client */ ;
    /*!50003 SET @saved_cs_results     = @@character_set_results */ ;
    /*!50003 SET @saved_col_connection = @@collation_connection */ ;
    /*!50003 SET character_set_client  = utf8 */ ;
    /*!50003 SET character_set_results = utf8 */ ;
    /*!50003 SET collation_connection  = utf8_general_ci */ ;
    /*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
    /*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_AUTO_CREATE_USER' */ ;
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
    
    --
    -- Current Database: `sakila`
    --
    
    USE `sakila`;
    
    --
    -- Final view structure for view `actor_info`
    --
    
    /*!50001 DROP TABLE IF EXISTS `actor_info`*/;
    /*!50001 DROP VIEW IF EXISTS `actor_info`*/;
    /*!50001 SET @saved_cs_client          = @@character_set_client */;
    /*!50001 SET @saved_cs_results         = @@character_set_results */;
    /*!50001 SET @saved_col_connection     = @@collation_connection */;
    /*!50001 SET character_set_client      = utf8 */;
    /*!50001 SET character_set_results     = utf8 */;
    /*!50001 SET collation_connection      = utf8_general_ci */;
    /*!50001 CREATE ALGORITHM=UNDEFINED */
    /*!50013 DEFINER=`root`@`localhost` SQL SECURITY INVOKER */
    /*!50001 VIEW `actor_info` AS select `a`.`actor_id` AS `actor_id`,`a`.`first_name` AS `first_name`,`a`.`last_name` AS `last_name`,
    group_concat(distinct concat(`c`.`name`,_utf8': ',(select group_concat(`f`.`title` order by `f`.`title` ASC separator ', ') AS `GROUP_CONCAT(f.title ORDER BY f.title SEPARATOR ', ')` from ((`film` `f` join `film_category` `fc` on((`f`.`film_id` = `fc`.`film_id`))) join `film_actor` `fa` on((`f`.`film_id` = `fa`.`film_id`))) where ((`fc`.`category_id` = `c`.`category_id`) and (`fa`.`actor_id` = `a`.`actor
    _id`)))) order by `c`.`name` ASC separator '; ') AS `film_info` from (((`actor` `a` left join `film_actor` `fa` on((`a`.`actor_id` = `fa`.`actor_id`))) left join `film_category` `
    fc` on((`fa`.`film_id` = `fc`.`film_id`))) left join `category` `c` on((`fc`.`category_id` = `c`.`category_id`))) group by `a`.`actor_id`,`a`.`first_name`,`a`.`last_name` */;
    /*!50001 SET character_set_client      = @saved_cs_client */;
    /*!50001 SET character_set_results     = @saved_cs_results */;
    /*!50001 SET collation_connection      = @saved_col_connection */;

    """).lstrip()

    stream = NodeStream(text.splitlines(True))
    expected_types = [
        'dump-header',
        'setup-session',
        'replication',
        'database-ddl',
        'table-ddl',
        'table-dml',
        'view-temp-ddl',
        'database-routines',
        'view-finalize-db',
        'view-ddl',
        'final',
    ]

    for i, node in enumerate(stream):
        assert_equals(node.type, expected_types[i])

def test_invalid_node_steram():
    # some invalid mysqldump file - probably not created by mysqldump 
    text = textwrap.dedent("""
    -- MySQL dump 10.13  Distrib 5.1.42, for redhat-linux-gnu (x86_64)
    --
    -- Host: localhost    Database: sakila
    -- ------------------------------------------------------
    -- Server version       5.1.42-rs-log

    /*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
    /*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
    /*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
    /*!40101 SET NAMES utf8 */;
    /*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
    /*!40103 SET TIME_ZONE='+00:00' */;
    /*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
    /*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
    /*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
    /*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

    DELIMITER ;;
    /*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `upd_film` AFTER UPDATE ON `film` FOR EACH ROW BEGIN
    DELIMITER ;

    -- Dump completed on 2010-04-22 14:44:42
    """).lstrip()

    stream = NodeStream(text.splitlines(True))
    i = iter(stream)

    assert_equals(i.next().type, 'dump-header')
    assert_equals(i.next().type, 'setup-session')
    assert_raises(ValueError, i.next)
