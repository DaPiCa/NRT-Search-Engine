FROM ubuntu/mysql:latest

COPY . .

RUN mkdir /var/log/mysql/ && chmod 777 /var/log/mysql/ -R

COPY SQL_Schema.sql /docker-entrypoint-initdb.d/
COPY custom.cnf /etc/mysql/conf.d/