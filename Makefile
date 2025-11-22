.PHONY: fmt
fmt:
	go fmt ./...

test-unit:
	go test -short ./...

test-integration:
	go test -run TestIntegration ./...

.PHONY: test-unit test-integration

.PHONY: docker-image
docker-image:
	docker build -t turbolytics.io/turboops .

.PHONY: start-backing-services
start-backing-services:
	@echo "Starting backing services..."
	@docker-compose -f dev/compose.yml up -d

.PHONY: stop-backing-services
stop-backing-services:
	@echo "Stopping backing services..."
	@docker-compose -f dev/compose.yml down --remove-orphans

.PHONY: build
build:
	go build -o bin/librarian ./cmd/librarian

.PHONY: release-dry-run
release-dry-run:
	goreleaser release --snapshot --clean --skip=publish