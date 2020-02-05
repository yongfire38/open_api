show databases;

use DB_TEST;

show tables;

create database DB_TEST default character set utf8 collate utf8_general_ci;

CREATE TABLE `project_code` (
`project_code`  varchar(100) NOT NULL ,
`project_gubun`  varchar(100) NOT NULL ,
`project_name`  varchar(100) NOT NULL ,
`x_location`  varchar(100) NOT NULL ,
`y_location`  varchar(100) NOT NULL ,
PRIMARY KEY (`project_code`) 
) default character set utf8 collate utf8_general_ci;

SELECT @@global.time_zone, @@session.time_zone;

select project_code from project_code;

select * from project_code
where project_code = 'WJ2005E011';

select count(*) from project_code;


