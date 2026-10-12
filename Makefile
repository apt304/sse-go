## test: Run all project tests
.PHONY: test
test:
	@echo "Running tests..."
	go test -race -count=1 ./...

.PHONY: test-bake
test-bake:
	@echo "Running tests with count=20"
	go test -race -count=20 ./...

.PHONY: test-cover
test-cover:
	@echo "Running tests with coverage..."
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: bench
bench:
	@echo "Running benchmarks"
	go test -bench=. -benchmem -run="^$$" ./...

.PHONY: fuzz-subscriber-30
fuzz-subscriber-30:
	@echo "Running fuzz FuzzDropOldestSubscriber_Matrix 30s"
	go test -race -run='^$$' -fuzz='^FuzzDropOldestSubscriber_Matrix$$' -fuzztime=30s ./server

.PHONY: fuzz-server-30
fuzz-server-30:
	@echo "Running fuzz FuzzServer_DropOldestSubscriber_Matrix 30s"
	go test -race -run='^$$' -fuzz='^FuzzServer_DropOldestSubscriber_Matrix$$' -fuzztime=30s ./server

.PHONY: fuzz-subscriber
fuzz-subscriber:
	@echo "Running fuzz FuzzDropOldestSubscriber_Matrix"
	go test -run='^$$' -fuzz='^FuzzDropOldestSubscriber_Matrix$$' ./server

.PHONY: fuzz-server
fuzz-server:
	@echo "Running fuzz FuzzServer_DropOldestSubscriber_Matrix"
	go test -run='^$$' -fuzz='^FuzzServer_DropOldestSubscriber_Matrix$$' ./server

## fmt: Format code using go fmt
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	go fmt ./...

## vet: Run go vet linting
.PHONY: vet
vet:
	@echo "Vetting code..."
	go vet ./...

## tidy: Add missing and remove unused modules
.PHONY: tidy
tidy:
	@echo "Tidying up modules..."
	go mod tidy
