.PHONY: openapi
openapi:
	./hack/openapi.sh

.PHONY: build
build:
	go build -o ./bin/main ./main.go

.PHONY: start
start: build
	./hack/start_simulator.sh
