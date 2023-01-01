-- MySQL dump 10.13  Distrib 8.0.31, for Win64 (x86_64)
--
-- Host: localhost    Database: speedtest
-- ------------------------------------------------------
-- Server version	8.0.31

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `brod_speed`
--

DROP TABLE IF EXISTS `brod_speed`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `brod_speed` (
  `Rank` int DEFAULT NULL,
  `Country` varchar(35) DEFAULT NULL,
  `Speed` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `brod_speed`
--

LOCK TABLES `brod_speed` WRITE;
/*!40000 ALTER TABLE `brod_speed` DISABLE KEYS */;
INSERT INTO `brod_speed` VALUES (1,'Chile','216.46'),(2,'China','214.58'),(3,'Singapore','214.23'),(4,'Thailand','205.63'),(5,'Hong Kong (SAR)','194.35'),(6,'United States','189.48'),(7,'Denmark','188.35'),(8,'United Arab Emirates','186.76'),(9,'Monaco','181.26'),(10,'Romania','170.25'),(11,'Spain','166.78'),(12,'Macau (SAR)','159.78'),(13,'Japan','150.32'),(14,'France','150.15'),(15,'Switzerland','144.78'),(16,'Canada','136.87'),(17,'New Zealand','133.18'),(18,'Andorra','131.30'),(19,'Taiwan','124.14'),(20,'Hungary','123.17'),(21,'Netherlands','120.82'),(22,'Portugal','119.06'),(23,'Panama','118.01'),(24,'Kuwait','117.26'),(25,'Norway','111.55'),(26,'Liechtenstein','110.97'),(27,'Sweden','107.83'),(28,'Moldova','105.06'),(29,'Uruguay','104.72'),(30,'Brazil','97.52'),(31,'Poland','95.95'),(32,'Israel','95.66'),(33,'South Korea','95.34'),(34,'Luxembourg','94.39'),(35,'Finland','94.39'),(36,'Malta','93.17'),(37,'Malaysia','91.50'),(38,'Qatar','90.86'),(39,'Colombia','89.61'),(40,'Lithuania','89.23'),(41,'Saudi Arabia','88.12'),(42,'Belgium','86.70'),(43,'Trinidad and Tobago','84.84'),(44,'Dominica','82.11'),(45,'Philippines','81.42'),(46,'Germany','80.69'),(47,'Vietnam','80.27'),(48,'Slovenia','79.29'),(49,'Barbados','78.27'),(50,'Ireland','77.91'),(51,'Saint Vincent and the Grenadines','76.59'),(52,'Saint Lucia','75.16'),(53,'Russia','74.92'),(54,'Jordan','74.46'),(55,'United Kingdom','73.21'),(56,'Latvia','72.40'),(57,'Paraguay','68.24'),(58,'Bulgaria','67.76'),(59,'Austria','67.23'),(60,'Peru','65.73'),(61,'Costa Rica','65.30'),(62,'Grenada','64.02'),(63,'San Marino','61.32'),(64,'Serbia','60.45'),(65,'Argentina','60.25'),(66,'Italy','60.12'),(67,'Ukraine','60.00'),(68,'Oman','59.74'),(69,'Slovakia','59.08'),(70,'Estonia','58.33'),(71,'Montenegro','54.66'),(72,'Kosovo','53.81'),(73,'Australia','53.06'),(74,'Czechia','52.78'),(75,'Mongolia','52.47'),(76,'Palestine','50.45'),(77,'Belarus','50.27'),(78,'Mexico','49.91'),(79,'Guyana','49.78'),(80,'India','49.09'),(81,'Jamaica','48.91'),(82,'Kyrgyzstan','48.47'),(83,'Nepal','48.45'),(84,'Bahrain','48.32'),(85,'Brunei','46.92'),(86,'Croatia','46.53'),(87,'Ecuador','46.32'),(88,'Egypt','45.79'),(89,'Kazakhstan','45.27'),(90,'Uzbekistan','45.17'),(91,'The Bahamas','45.11'),(92,'Burkina Faso','44.78'),(93,'Albania','44.63'),(94,'Greece','42.79'),(95,'Nicaragua','41.41'),(96,'Belize','40.72'),(97,'Armenia','40.14'),(98,'South Africa','40.12'),(99,'Gabon','39.72'),(100,'Côte d\'Ivoire','38.92'),(101,'Cyprus','36.50'),(102,'Bangladesh','34.85'),(103,'North Macedonia','33.37'),(104,'Tajikistan','32.09'),(105,'Turkey','31.86'),(106,'El Salvador','31.30'),(107,'Seychelles','30.86'),(108,'Madagascar','30.62'),(109,'Togo','29.63'),(110,'Congo','29.58'),(111,'Saint Kitts and Nevis','29.54'),(112,'Honduras','29.40'),(113,'Laos','28.33'),(114,'Ghana','27.70'),(115,'Bosnia and Herzegovina','27.25'),(116,'Antigua and Barbuda','27.14'),(117,'Guatemala','27.02'),(118,'Azerbaijan','26.95'),(119,'Mauritius','26.53'),(120,'Georgia','25.97'),(121,'Bolivia','24.94'),(122,'Indonesia','24.32'),(123,'Mauritania','23.74'),(124,'Mali','22.88'),(125,'Senegal','21.77'),(126,'Iraq','21.27'),(127,'Cambodia','20.22'),(128,'Sri Lanka','20.09'),(129,'Dominican Republic','19.28'),(130,'Myanmar (Burma)','18.98'),(131,'Morocco','17.97'),(132,'Lesotho','17.33'),(133,'Bhutan','17.27'),(134,'Haiti','15.71'),(135,'Western Sahara','15.14'),(136,'Angola','14.89'),(137,'Cape Verde','14.74'),(138,'Benin','14.55'),(139,'Venezuela','14.49'),(140,'Fiji','14.04'),(141,'Tanzania','12.99'),(142,'Papua New Guinea','11.94'),(143,'Nigeria','11.84'),(144,'Iran','11.62'),(145,'Sierra Leone','11.60'),(146,'Suriname','11.49'),(147,'Algeria','11.01'),(148,'Uganda','10.72'),(149,'Maldives','10.45'),(150,'Pakistan','10.28'),(151,'DR Congo','10.24'),(152,'Rwanda','10.15'),(153,'Kenya','9.60'),(154,'Vanuatu','9.40'),(155,'Libya','8.86'),(156,'Malawi','8.77'),(157,'Zambia','8.66'),(158,'Zimbabwe','8.52'),(159,'Guinea','8.48'),(160,'Cameroon','8.41'),(161,'Somalia','8.34'),(162,'Namibia','8.28'),(163,'Tunisia','8.15'),(164,'Botswana','7.52'),(165,'Djibouti','7.36'),(166,'Liberia','7.35'),(167,'Lebanon','7.27'),(168,'The Gambia','6.44'),(169,'Mozambique','6.29'),(170,'Ethiopia','5.19'),(171,'Swaziland','5.16'),(172,'Sudan','5.04'),(173,'Burundi','4.90'),(174,'Niger','4.47'),(175,'Yemen','3.90'),(176,'Syria','3.16'),(177,'South Sudan','2.81'),(178,'Turkmenistan','2.45'),(179,'Afghanistan','2.25'),(180,'Cuba','1.91');
/*!40000 ALTER TABLE `brod_speed` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-01-01 19:38:26
