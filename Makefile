TARGET_DIR?=$(shell pwd)
GITHUB_GROUP=github.com/th2-net

TH2_COMMON_GO=th2-common-go
TH2_COMMON_GO_URL=$(GITHUB_GROUP)/$(TH2_COMMON_GO)@rabbitMQ_dev

clean-deps:
	-rm go.work

deps: clean-deps 
	go work init .

	go get -u -t $(TH2_COMMON_GO_URL)
	sleep 4
	@cd $(subst \,/, $(shell go list -m -f '{{.Dir}}' $(TH2_COMMON_GO_URL))) && echo $(shell ls)
	@cd $(subst \,/, $(shell go list -m -f '{{.Dir}}' $(TH2_COMMON_GO_URL))) && make deps TARGET_DIR=$(TARGET_DIR)