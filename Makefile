default: build

tidy:
	go mod tidy -v

build:
	go vet ./...
	go build -v

run-test:
	go test -v -race ./...