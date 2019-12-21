-- Set up users
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;

CREATE USER 'spark'@'localhost' IDENTIFIED WITH mysql_native_password BY 'P18YtrJj8q6ioevT';
GRANT ALL PRIVILEGES ON *.* TO 'spark'@'localhost' WITH GRANT OPTION;
CREATE USER 'spark'@'%' IDENTIFIED WITH mysql_native_password BY 'P18YtrJj8q6ioevT';
GRANT ALL PRIVILEGES ON *.* TO 'spark'@'%' WITH GRANT OPTION;

CREATE USER 'client'@'localhost' IDENTIFIED WITH mysql_native_password BY 'H8IAQzX236eu5Ep0';
GRANT SELECT ON *.* TO 'client'@'localhost';
CREATE USER 'client'@'%' IDENTIFIED WITH mysql_native_password BY 'H8IAQzX236eu5Ep0';
GRANT SELECT ON *.* TO 'client'@'%';

FLUSH PRIVILEGES;

-- Create databases
CREATE DATABASE analysis_results;
CREATE DATABASE flume;

-- Set up database for analysis results
USE analysis_results;

create table service_cases_daily(
	`neighborhood` varchar(128) not null,
	`category` varchar(256) not null,
	`rate` double precision not null,
	`day` date not null,
	primary key (`neighborhood`, `category`, `day`)
);

create table service_cases_monthly(
	`neighborhood` varchar(128) not null,
	`category` varchar(256) not null,
	`rate` double precision not null,
	`month` DATE not null,
	primary key (`neighborhood`, `category`, `month`)
);

create table incident_cases_daily
(
	`neighborhood` varchar(128) not null,
	`category` varchar(256) not null,
	`rate` double precision not null,
	`day` date not null,
	primary key (`neighborhood`, `category`, `day`)
);

create table incident_cases_monthly
(
	`neighborhood` varchar(128) not null,
	`category` varchar(256) not null,
	`rate` double precision not null,
	`month` DATE not null,
	primary key (`neighborhood`, `category`, `month`)
);

-- Set up database for flume
USE flume;

create table data_ingestion_latest(
	`data_source` char(9) not null primary key,
	`latest` datetime not null
);
