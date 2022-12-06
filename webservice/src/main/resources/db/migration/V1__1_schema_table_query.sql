CREATE TABLE `sdl_schema`
(
    `id`   bigint(20)  NOT NULL AUTO_INCREMENT,
    `name` varchar(50) NOT NULL,
    UNIQUE KEY `uk_schema_name` (`name`),
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE `sdl_table`
(
    `id`         bigint(20)  NOT NULL AUTO_INCREMENT,
    `name`       varchar(50) NOT NULL,
    `schema_id`  bigint(20)  NOT NULL,
    `statistics` TEXT        NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_schema_id_name` (`name`, `schema_id`),
    KEY `schema_id` (`schema_id`),
    CONSTRAINT `fk_schema_to_table` FOREIGN KEY (`schema_id`) REFERENCES `sdl_schema` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
