FROM golang:1.19
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN go build -o main src/*.go
CMD ["/app/main"]