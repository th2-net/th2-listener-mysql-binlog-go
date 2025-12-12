FROM ubuntu:latest
WORKDIR /app
COPY ./th2-listener-mysql-binlog-go service
ENTRYPOINT ["/app/service"]