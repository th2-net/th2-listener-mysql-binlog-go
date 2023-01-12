FROM golang:1.19
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN apt update && apt install -y make
RUN make
RUN go build -o main main.go
CMD ["/app/main"]