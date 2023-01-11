FROM golang:1.19
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN RUN apt update && apt install -y make
RUN make deps
RUN go build -o main src/*.go
CMD ["/app/main"]