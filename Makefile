SRC_MAIN_PROTO_DIR=src/main/proto
GITHUB_TH2=github.com/th2-net

TH2_GRPC_COMMON=th2-grpc-common
TH2_GRPC_COMMON_URL=$(GITHUB_TH2)/$(TH2_GRPC_COMMON)@makefile

MODULE_NAME=th2-grpc
MODULE_DIR=$(MODULE_NAME)

PROTOBUF_VERSION=v1.5.2

PROTOC_VERSION=21.12

default: prepare-main-module build

configure-go:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

clean-grpc-module:
	-rm -rf $(MODULE_DIR)

prepare-grpc-module: clean-grpc-module
	mkdir $(MODULE_DIR)
	cd $(MODULE_DIR) && go mod init $(MODULE_NAME)

	cd $(MODULE_DIR) \
		&& go get -u -t $(TH2_GRPC_COMMON_URL) \
		&& go get -u -t google.golang.org/protobuf@v1.26.0 \
		&& go get -u -t github.com/google/go-cmp@v0.5.9

	- go work init
	go work use ./$(MODULE_DIR)

generate-grpc-files: prepare-grpc-module configure-go
	$(eval $@_PROTO_DIR := $(shell go list -m -f '{{.Dir}}' $(TH2_GRPC_COMMON_URL))/$(SRC_MAIN_PROTO_DIR))
	protoc \
		--go_out=$(MODULE_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(MODULE_DIR) \
		--go-grpc_opt=paths=source_relative \
		--proto_path=$($@_PROTO_DIR) \
		$(shell find $($@_PROTO_DIR) -name '*.proto' )
	cd $(MODULE_NAME) && go mod tidy

prepare-main-module: generate-grpc-files
	- go work init
	go work use .

build:
	go vet ./...
	go build -v -race ./...

run-test:
	go test -v ./...