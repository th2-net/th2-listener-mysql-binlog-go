FROM golang:1.19 AS build
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN apt update && apt install -y make
RUN make
RUN go build -o main main.go

FROM alpine:3.14
WORKDIR /app
COPY --from=build /app/main .
CMD ["/app/main"]