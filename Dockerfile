FROM golang:1.23 AS build
RUN apt update \
    && apt install -y make
WORKDIR /app
ADD . /app
RUN make build run-test
RUN go build -o main .

FROM ubuntu:latest
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["/app/main"]