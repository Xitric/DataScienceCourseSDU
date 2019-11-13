FROM mysql/mysql-server:latest

ENV MYSQL_ROOT_PASSWORD=password

COPY init.sql /docker-entrypoint-initdb.d/

EXPOSE 3306
