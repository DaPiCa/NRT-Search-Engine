FROM ubuntu/mysql:latest
ENV MYSQL_ROOT_PASSWORD=user
ENV MYSQL_USER=user
ENV MYSQL_PASSWORD=user

COPY . .

COPY SQL_Schema.sql /docker-entrypoint-initdb.d/