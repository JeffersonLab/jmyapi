CREATE TABLE `channels` (
                            `chan_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                            `name` varchar(255) COLLATE latin1_general_cs NOT NULL,
                            `type` tinyint(3) unsigned NOT NULL,
                            `adel` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,
                            `size` smallint(5) unsigned NOT NULL DEFAULT '1',
                            `clip` int(10) unsigned DEFAULT NULL,
                            `active` tinyint(4) NOT NULL DEFAULT '1',
                            `request` tinyint(4) NOT NULL DEFAULT '0',
                            `alert` tinyint(4) NOT NULL DEFAULT '0',
                            `host` varchar(255) COLLATE latin1_general_cs NOT NULL,
                            `backup` bigint(20) DEFAULT '0',
                            `ioc` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,
                            PRIMARY KEY (`chan_id`),
                            UNIQUE KEY `name` (`name`),
                            KEY `host` (`host`(16)),
                            KEY `alert` (`alert`)
);

CREATE TABLE `metadata` (
                            `chan_id` int(10) unsigned NOT NULL,
                            `keyword` varchar(255) COLLATE latin1_general_cs NOT NULL,
                            `stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            `info` text COLLATE latin1_general_cs NOT NULL,
                            KEY `chan_id` (`chan_id`),
                            KEY `keyword` (`keyword`(16)),
                            CONSTRAINT `metadata_ibfk_1` FOREIGN KEY (`chan_id`) REFERENCES `channels` (`chan_id`) ON DELETE CASCADE
);

CREATE TABLE `table_1` (
                               `time` bigint(20) NOT NULL,
                               `code` tinyint(3) unsigned NOT NULL DEFAULT '0',
                               `val1` float NOT NULL DEFAULT '0',
                               PRIMARY KEY (`time`)
);

CREATE TABLE `table_2` (
                               `time` bigint(20) NOT NULL,
                               `code` tinyint(3) unsigned NOT NULL DEFAULT '0',
                               `val1` smallint(5) unsigned NOT NULL DEFAULT '0',
                               PRIMARY KEY (`time`)
);