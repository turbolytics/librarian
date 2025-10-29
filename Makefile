.PHONY: fmt
fmt:
	go fmt ./...

test-unit:
	go test -short ./...

test-integration:
	go test -run TestIntegration ./...

.PHONY: test-unit test-integration