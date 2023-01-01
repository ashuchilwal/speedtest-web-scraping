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
-- Table structure for table `mobile_speed`
--

DROP TABLE IF EXISTS `mobile_speed`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mobile_speed` (
  `Rank` int DEFAULT NULL,
  `Country` varchar(35) DEFAULT NULL,
  `Speed` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mobile_speed`
--

LOCK TABLES `mobile_speed` WRITE;
/*!40000 ALTER TABLE `mobile_speed` DISABLE KEYS */;
INSERT INTO `mobile_speed` VALUES (1,'Qatar','176.18'),(2,'United Arab Emirates','139.41'),(3,'Norway','131.54'),(4,'South Korea','118.76'),(5,'Denmark','113.44'),(6,'China','109.40'),(7,'Netherlands','109.06'),(8,'Macau (SAR)','106.38'),(9,'Bulgaria','103.29'),(10,'Brunei','102.06'),(11,'Kuwait','97.66'),(12,'Saudi Arabia','90.49'),(13,'Australia','86.01'),(14,'Sweden','85.64'),(15,'Bahrain','84.22'),(16,'Finland','83.61'),(17,'Canada','82.68'),(18,'Switzerland','77.36'),(19,'Luxembourg','76.75'),(20,'United States','74.83'),(21,'Singapore','72.18'),(22,'Maldives','71.40'),(23,'Croatia','70.27'),(24,'Taiwan','68.04'),(25,'North Macedonia','65.88'),(26,'Cyprus','64.26'),(27,'Portugal','62.95'),(28,'Greece','61.40'),(29,'Austria','61.19'),(30,'France','59.66'),(31,'Estonia','59.46'),(32,'Lithuania','58.30'),(33,'New Zealand','56.64'),(34,'Hong Kong (SAR)','56.51'),(35,'Belgium','56.18'),(36,'Germany','56.18'),(37,'Slovakia','54.41'),(38,'Oman','52.35'),(39,'Slovenia','50.75'),(40,'Montenegro','49.34'),(41,'Malta','48.67'),(42,'Serbia','46.50'),(43,'Latvia','46.10'),(44,'United Kingdom','46.02'),(45,'Czechia','44.46'),(46,'Hungary','41.11'),(47,'Romania','41.09'),(48,'Iran','40.97'),(49,'Poland','40.92'),(50,'Japan','40.92'),(51,'Vietnam','39.59'),(52,'Albania','39.29'),(53,'Italy','39.25'),(54,'Thailand','37.85'),(55,'South Africa','36.70'),(56,'Malaysia','36.34'),(57,'Spain','36.07'),(58,'Azerbaijan','34.60'),(59,'Georgia','33.67'),(60,'Brazil','33.36'),(61,'Uruguay','33.12'),(62,'Botswana','33.05'),(63,'Turkey','31.93'),(64,'Morocco','31.65'),(65,'Jamaica','30.79'),(66,'Iraq','30.62'),(67,'Moldova','28.64'),(68,'Laos','28.56'),(69,'Lebanon','28.39'),(70,'Trinidad and Tobago','27.89'),(71,'Israel','27.18'),(72,'Uganda','26.07'),(73,'Chile','25.85'),(74,'Guatemala','25.54'),(75,'Togo','25.50'),(76,'Myanmar (Burma)','25.21'),(77,'Kosovo','25.18'),(78,'Ireland','24.84'),(79,'Bosnia and Herzegovina','24.74'),(80,'Mexico','24.07'),(81,'Philippines','24.04'),(82,'Senegal','23.10'),(83,'Honduras','22.69'),(84,'Armenia','22.35'),(85,'Argentina','22.17'),(86,'Egypt','22.17'),(87,'Mauritius','21.97'),(88,'Russia','21.73'),(89,'Tunisia','21.42'),(90,'Kazakhstan','21.05'),(91,'El Salvador','20.98'),(92,'Kenya','20.41'),(93,'Dominican Republic','20.40'),(94,'Nigeria','19.84'),(95,'Angola','19.51'),(96,'Fiji','19.43'),(97,'Kyrgyzstan','19.14'),(98,'Mozambique','19.13'),(99,'Cambodia','19.12'),(100,'Ethiopia','19.10'),(101,'Ecuador','18.83'),(102,'Mongolia','18.37'),(103,'Nicaragua','18.37'),(104,'Namibia','18.30'),(105,'India','18.26'),(106,'Côte d\'Ivoire','18.25'),(107,'Costa Rica','17.47'),(108,'Paraguay','17.45'),(109,'Indonesia','17.27'),(110,'Peru','16.62'),(111,'Jordan','16.60'),(112,'DR Congo','15.86'),(113,'Zambia','15.14'),(114,'Pakistan','14.93'),(115,'Uzbekistan','14.55'),(116,'Tanzania','14.48'),(117,'Libya','14.48'),(118,'Sri Lanka','14.27'),(119,'Bangladesh','13.95'),(120,'Panama','13.92'),(121,'Nepal','13.49'),(122,'Algeria','13.40'),(123,'Sudan','13.38'),(124,'Burkina Faso','12.60'),(125,'Belarus','11.71'),(126,'Somalia','11.65'),(127,'Ukraine','11.23'),(128,'Mauritania','10.92'),(129,'Zimbabwe','10.88'),(130,'Colombia','10.82'),(131,'Cameroon','10.55'),(132,'Bolivia','10.16'),(133,'Belize','9.73'),(134,'Yemen','9.63'),(135,'Syria','9.27'),(136,'Tajikistan','8.17'),(137,'Ghana','7.90'),(138,'Papua New Guinea','6.75'),(139,'Haiti','6.30'),(140,'Cuba','5.74'),(141,'Venezuela','5.71'),(142,'Afghanistan','5.27');
/*!40000 ALTER TABLE `mobile_speed` ENABLE KEYS */;
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
