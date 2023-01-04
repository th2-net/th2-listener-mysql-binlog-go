FROM golang:1.19
RUN mkdir /app
ADD . /app
WORKDIR /app/src
RUN go build -o .
CMD ["/src"]