CREATE TABLE `storage` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `timestamp` int(11) NOT NULL,
  `pool` varchar(255) NOT NULL,
  `total` BIGINT UNSIGNED NOT NULL,
  `used` BIGINT UNSIGNED NOT NULL,
  `available` BIGINT UNSIGNED NOT NULL,
  `db_sizes` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  INDEX (`timestamp`)
) ENGINE=InnoDB;