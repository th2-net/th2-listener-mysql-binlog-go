FROM golang:1.19 AS build
WORKDIR /app
ADD . /app
RUN apt update && apt install -y make
RUN make
RUN go build -o main .

FROM ubuntu:latest
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["/app/main"]