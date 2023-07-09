FROM ubuntu/mysql:latest

COPY . .

RUN mkdir /var/log/mysql/ && chmod 777 /var/log/mysql/ -R

COPY sample.sql /docker-entrypoint-initdb.d/
COPY custom.cnf /etc/mysql/conf.d/