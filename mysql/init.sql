-- Set up users
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;

CREATE USER 'spark'@'localhost' IDENTIFIED BY 'P18YtrJj8q6ioevT';
GRANT ALL PRIVILEGES ON *.* TO 'spark'@'localhost' WITH GRANT OPTION;
CREATE USER 'spark'@'%' IDENTIFIED BY 'P18YtrJj8q6ioevT';
GRANT ALL PRIVILEGES ON *.* TO 'spark'@'%' WITH GRANT OPTION;

CREATE USER 'client'@'localhost' IDENTIFIED BY 'H8IAQzX236eu5Ep0';
GRANT SELECT ON *.* TO 'client'@'localhost';
CREATE USER 'client'@'%' IDENTIFIED BY 'H8IAQzX236eu5Ep0';
GRANT SELECT ON *.* TO 'client'@'%';

FLUSH PRIVILEGES;

-- Set up database for analysis results
CREATE DATABASE analysis_results;
USE analysis_results;

-- Set up database for flume
CREATE DATABASE flume;
USE flume;
create table data_ingestion_latest
(
    data_source char(9)  not null
        primary key,
    latest      datetime not null
);