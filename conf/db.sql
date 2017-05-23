create database `d_finance`;

USE `d_finance`;

CREATE TABLE `t_real_time_info`(
  `c_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `c_name` varchar(128) NOT NULL,
  `c_code` varchar(64) NOT NULL,
  `c_open` float NOT NULL,
  `c_pre_close` float NOT NULL,
  `c_price` float NOT NULL,
  `c_high` float NOT NULL,
  `c_low` float NOT NULL,
  `c_bid` float NOT NULL,
  `c_ask` float NOT NULL,
  `c_volume` bigint(20) unsigned NOT NULL,
  `c_amount` double NOT NULL,
  `c_b1_v` int(10) unsigned NOT NULL,
  `c_b1_p` float NOT NULL,
  `c_b2_v` int(10) unsigned NOT NULL,
  `c_b2_p` float NOT NULL,
  `c_b3_v` int(10) unsigned NOT NULL,
  `c_b3_p` float NOT NULL,
  `c_b4_v` int(10) unsigned NOT NULL,
  `c_b4_p` float NOT NULL,
  `c_b5_v` int(10) unsigned NOT NULL,
  `c_b5_p` float NOT NULL,
  `c_a1_v` int(10) unsigned NOT NULL,
  `c_a1_p` float NOT NULL,
  `c_a2_v` int(10) unsigned NOT NULL,
  `c_a2_p` float NOT NULL,
  `c_a3_v` int(10) unsigned NOT NULL,
  `c_a3_p` float NOT NULL,
  `c_a4_v` int(10) unsigned NOT NULL,
  `c_a4_p` float NOT NULL,
  `c_a5_v` int(10) unsigned NOT NULL,
  `c_a5_p` float NOT NULL,
  `c_date_time` datetime NOT NULL,
  `c_modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `c_create_time` datetime NOT NULL,
  PRIMARY KEY (`c_id`),
  KEY `code` (`c_code`),
  UNIQUE KEY `code_date_time` (`c_date_time`, `c_code`),
  KEY `modify_time` (`c_modify_time`),
  KEY `create_time` (`c_create_time`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `t_stock_tick_info`(
  `c_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `c_code` varchar(64) NOT NULL,
  `c_price` float NOT NULL,
  `c_pchange` float NOT NULL,
  `c_change` float NOT NULL,
  `c_volume` bigint(20) unsigned NOT NULL,
  `c_amount` double NOT NULL,
  `c_type` varchar(64) NOT NULL,
  `c_date_time` datetime NOT NULL,
  `c_modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `c_create_time` datetime NOT NULL,
  PRIMARY KEY (`c_id`),
  KEY `code` (`c_code`),
  UNIQUE KEY `code_date_time` (`c_date_time`, `c_code`),
  KEY `modify_time` (`c_modify_time`),
  KEY `create_time` (`c_create_time`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `t_stock_info`(
  `c_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `c_code` varchar(64) NOT NULL,
  `c_industry` varchar(64) NOT NULL,
  `c_area` varchar(64) NOT NULL,
  `c_concept` varchar(64) NOT NULL,
  `c_modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `c_create_time` datetime NOT NULL,
  PRIMARY KEY (`c_id`),
  UNIQUE KEY `code` (`c_code`),
  KEY `modify_time` (`c_modify_time`),
  KEY `create_time` (`c_create_time`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
