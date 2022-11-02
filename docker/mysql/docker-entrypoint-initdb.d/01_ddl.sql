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

CREATE TABLE `table_3` (
                              `time` bigint(20) NOT NULL,
                              `code` tinyint(3) unsigned NOT NULL DEFAULT '0',
                              `val1` float NOT NULL DEFAULT '0',
                              `val2` float NOT NULL DEFAULT '0',
                              `val3` float NOT NULL DEFAULT '0',
                              `val4` float NOT NULL DEFAULT '0',
                              `val5` float NOT NULL DEFAULT '0',
                              `val6` float NOT NULL DEFAULT '0',
                              `val7` float NOT NULL DEFAULT '0',
                              `val8` float NOT NULL DEFAULT '0',
                              `val9` float NOT NULL DEFAULT '0',
                              `val10` float NOT NULL DEFAULT '0',
                              `val11` float NOT NULL DEFAULT '0',
                              `val12` float NOT NULL DEFAULT '0',
                              `val13` float NOT NULL DEFAULT '0',
                              `val14` float NOT NULL DEFAULT '0',
                              `val15` float NOT NULL DEFAULT '0',
                              `val16` float NOT NULL DEFAULT '0',
                              `val17` float NOT NULL DEFAULT '0',
                              `val18` float NOT NULL DEFAULT '0',
                              `val19` float NOT NULL DEFAULT '0',
                              `val20` float NOT NULL DEFAULT '0',
                              `val21` float NOT NULL DEFAULT '0',
                              `val22` float NOT NULL DEFAULT '0',
                              `val23` float NOT NULL DEFAULT '0',
                              `val24` float NOT NULL DEFAULT '0',
                              `val25` float NOT NULL DEFAULT '0',
                              `val26` float NOT NULL DEFAULT '0',
                              `val27` float NOT NULL DEFAULT '0',
                              `val28` float NOT NULL DEFAULT '0',
                              `val29` float NOT NULL DEFAULT '0',
                              `val30` float NOT NULL DEFAULT '0',
                              `val31` float NOT NULL DEFAULT '0',
                              `val32` float NOT NULL DEFAULT '0',
                              `val33` float NOT NULL DEFAULT '0',
                              `val34` float NOT NULL DEFAULT '0',
                              `val35` float NOT NULL DEFAULT '0',
                              `val36` float NOT NULL DEFAULT '0',
                              `val37` float NOT NULL DEFAULT '0',
                              `val38` float NOT NULL DEFAULT '0',
                              `val39` float NOT NULL DEFAULT '0',
                              `val40` float NOT NULL DEFAULT '0',
                              `val41` float NOT NULL DEFAULT '0',
                              `val42` float NOT NULL DEFAULT '0',
                              `val43` float NOT NULL DEFAULT '0',
                              `val44` float NOT NULL DEFAULT '0',
                              `val45` float NOT NULL DEFAULT '0',
                              `val46` float NOT NULL DEFAULT '0',
                              `val47` float NOT NULL DEFAULT '0',
                              `val48` float NOT NULL DEFAULT '0',
                              `val49` float NOT NULL DEFAULT '0',
                              `val50` float NOT NULL DEFAULT '0',
                              `val51` float NOT NULL DEFAULT '0',
                              `val52` float NOT NULL DEFAULT '0',
                              `val53` float NOT NULL DEFAULT '0',
                              `val54` float NOT NULL DEFAULT '0',
                              `val55` float NOT NULL DEFAULT '0',
                              `val56` float NOT NULL DEFAULT '0',
                              `val57` float NOT NULL DEFAULT '0',
                              `val58` float NOT NULL DEFAULT '0',
                              `val59` float NOT NULL DEFAULT '0',
                              `val60` float NOT NULL DEFAULT '0',
                              `val61` float NOT NULL DEFAULT '0',
                              `val62` float NOT NULL DEFAULT '0',
                              `val63` float NOT NULL DEFAULT '0',
                              `val64` float NOT NULL DEFAULT '0',
                              `val65` float NOT NULL DEFAULT '0',
                              `val66` float NOT NULL DEFAULT '0',
                              `val67` float NOT NULL DEFAULT '0',
                              `val68` float NOT NULL DEFAULT '0',
                              `val69` float NOT NULL DEFAULT '0',
                              `val70` float NOT NULL DEFAULT '0',
                              `val71` float NOT NULL DEFAULT '0',
                              `val72` float NOT NULL DEFAULT '0',
                              `val73` float NOT NULL DEFAULT '0',
                              `val74` float NOT NULL DEFAULT '0',
                              `val75` float NOT NULL DEFAULT '0',
                              `val76` float NOT NULL DEFAULT '0',
                              `val77` float NOT NULL DEFAULT '0',
                              `val78` float NOT NULL DEFAULT '0',
                              `val79` float NOT NULL DEFAULT '0',
                              `val80` float NOT NULL DEFAULT '0',
                              `val81` float NOT NULL DEFAULT '0',
                              `val82` float NOT NULL DEFAULT '0',
                              `val83` float NOT NULL DEFAULT '0',
                              `val84` float NOT NULL DEFAULT '0',
                              `val85` float NOT NULL DEFAULT '0',
                              `val86` float NOT NULL DEFAULT '0',
                              `val87` float NOT NULL DEFAULT '0',
                              `val88` float NOT NULL DEFAULT '0',
                              `val89` float NOT NULL DEFAULT '0',
                              `val90` float NOT NULL DEFAULT '0',
                              `val91` float NOT NULL DEFAULT '0',
                              `val92` float NOT NULL DEFAULT '0',
                              `val93` float NOT NULL DEFAULT '0',
                              `val94` float NOT NULL DEFAULT '0',
                              `val95` float NOT NULL DEFAULT '0',
                              `val96` float NOT NULL DEFAULT '0',
                              `val97` float NOT NULL DEFAULT '0',
                              `val98` float NOT NULL DEFAULT '0',
                              `val99` float NOT NULL DEFAULT '0',
                              `val100` float NOT NULL DEFAULT '0',
                              `val101` float NOT NULL DEFAULT '0',
                              `val102` float NOT NULL DEFAULT '0',
                              `val103` float NOT NULL DEFAULT '0',
                              `val104` float NOT NULL DEFAULT '0',
                              `val105` float NOT NULL DEFAULT '0',
                              `val106` float NOT NULL DEFAULT '0',
                              `val107` float NOT NULL DEFAULT '0',
                              `val108` float NOT NULL DEFAULT '0',
                              `val109` float NOT NULL DEFAULT '0',
                              `val110` float NOT NULL DEFAULT '0',
                              `val111` float NOT NULL DEFAULT '0',
                              `val112` float NOT NULL DEFAULT '0',
                              `val113` float NOT NULL DEFAULT '0',
                              `val114` float NOT NULL DEFAULT '0',
                              `val115` float NOT NULL DEFAULT '0',
                              `val116` float NOT NULL DEFAULT '0',
                              `val117` float NOT NULL DEFAULT '0',
                              `val118` float NOT NULL DEFAULT '0',
                              `val119` float NOT NULL DEFAULT '0',
                              `val120` float NOT NULL DEFAULT '0',
                              `val121` float NOT NULL DEFAULT '0',
                              `val122` float NOT NULL DEFAULT '0',
                              `val123` float NOT NULL DEFAULT '0',
                              `val124` float NOT NULL DEFAULT '0',
                              `val125` float NOT NULL DEFAULT '0',
                              `val126` float NOT NULL DEFAULT '0',
                              `val127` float NOT NULL DEFAULT '0',
                              `val128` float NOT NULL DEFAULT '0',
                              `val129` float NOT NULL DEFAULT '0',
                              `val130` float NOT NULL DEFAULT '0',
                              `val131` float NOT NULL DEFAULT '0',
                              `val132` float NOT NULL DEFAULT '0',
                              `val133` float NOT NULL DEFAULT '0',
                              `val134` float NOT NULL DEFAULT '0',
                              `val135` float NOT NULL DEFAULT '0',
                              `val136` float NOT NULL DEFAULT '0',
                              `val137` float NOT NULL DEFAULT '0',
                              `val138` float NOT NULL DEFAULT '0',
                              `val139` float NOT NULL DEFAULT '0',
                              `val140` float NOT NULL DEFAULT '0',
                              `val141` float NOT NULL DEFAULT '0',
                              `val142` float NOT NULL DEFAULT '0',
                              `val143` float NOT NULL DEFAULT '0',
                              `val144` float NOT NULL DEFAULT '0',
                              `val145` float NOT NULL DEFAULT '0',
                              `val146` float NOT NULL DEFAULT '0',
                              `val147` float NOT NULL DEFAULT '0',
                              `val148` float NOT NULL DEFAULT '0',
                              `val149` float NOT NULL DEFAULT '0',
                              `val150` float NOT NULL DEFAULT '0',
                              `val151` float NOT NULL DEFAULT '0',
                              `val152` float NOT NULL DEFAULT '0',
                              `val153` float NOT NULL DEFAULT '0',
                              `val154` float NOT NULL DEFAULT '0',
                              `val155` float NOT NULL DEFAULT '0',
                              `val156` float NOT NULL DEFAULT '0',
                              `val157` float NOT NULL DEFAULT '0',
                              `val158` float NOT NULL DEFAULT '0',
                              `val159` float NOT NULL DEFAULT '0',
                              `val160` float NOT NULL DEFAULT '0',
                              `val161` float NOT NULL DEFAULT '0',
                              `val162` float NOT NULL DEFAULT '0',
                              `val163` float NOT NULL DEFAULT '0',
                              `val164` float NOT NULL DEFAULT '0',
                              `val165` float NOT NULL DEFAULT '0',
                              `val166` float NOT NULL DEFAULT '0',
                              `val167` float NOT NULL DEFAULT '0',
                              `val168` float NOT NULL DEFAULT '0',
                              PRIMARY KEY (`time`)
);

DELIMITER //
CREATE DEFINER=`myapi`@`localhost` PROCEDURE `sample`(in tab char(20), in points int, in t1 bigint, in t2 bigint, in get int)
    COMMENT ' get - Return the data as a result set (non zero)'
begin
    declare i int default 0;
    declare start, stop bigint;
    declare step double default (t2-t1) / points;  # Time span length.

    # Create a temporary table, identical to the passed history table, to hold the
    # results from the user specified table. Note a session scope user variable is used
    # because the 'prepare' feature does not work with procedure variables.
    drop temporary table if exists table_x;
    set @stmt = 'create temporary table table_x select * from ';
    set @stmt = concat(@stmt,tab,' where time=0');
    prepare p from @stmt;
    execute p;
    deallocate prepare p;

    # Create a dynamic SQL statement for selecting a row from the channel history table
    # and inserting it into the temporary table.
    set @insert = 'insert into table_x select * from ';
    set @insert = concat(@insert,tab,' where time>=? and time<? and (code&15)=0 limit 1');
    prepare p from @insert;

    # Create a dynamic SQL statement for getting the nearest inbound channel event prior
    # to a time bin.
    set @prior = 'select code,val1 into @code, @value from ';
    set @prior = concat(@prior,tab,' force index(primary) where time<? order by time desc limit 1');
    prepare q from @prior;

    # Iterate for each of the time bins contained within the overall requested time
    # range.
    while i < points do
            set @start = t1 + i * step;    # Begin time of span
            set @stop = @start + step;     # End time of span

            # Query for a single row, within one time bin, from the source table; putting it
            # in the temporary table.
            execute p using @start, @stop;

            # Was the time bin empty?  If so, there may be an inbound value that persisted
            # throughout the time span of the bin. If an inbound channel event exists and
            # is not a discontinuity, insert the inbound value into the temporary table.
            select row_count() into @count;
            if @count = 0 then
                set @code = 1;
                execute q using @start;
                if (@code&15)=0 then
                    insert into table_x values (@start,0,@value);
                end if;
            end if;

            set i = i + 1;
        end while;

    # Free resources for the prepared statements.
    deallocate prepare q;
    deallocate prepare p;

# Was an immediate return of values requested?
    if get != 0 then
        select * from table_x order by time;   # Return the accumulated results to the user.
        drop temporary table table_x;          # Get rid of the table.
    end if;
end;
//
DELIMITER ;