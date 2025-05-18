.PHONY: all clean proto build run-leader run-follower run-client

all: proto build

# Build protobuf files
proto:
	@echo "Generating protobuf code..."
	@mkdir -p proto/
	@if [ -f proto/polyfs.proto ]; then \
		protoc --go_out=. --go_opt=paths=source_relative \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative \
			proto/polyfs.proto; \
	else \
		echo "proto/polyfs.proto not found"; \
		exit 1; \
	fi

# Build all binaries
build: proto
	@echo "Building node and client binaries..."
	@mkdir -p bin/
	go build -o bin/polyfs-node cmd/node/main.go
	go build -o bin/polyfs-client cmd/client/main.go

# Clean up
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -rf data/
	rm -f proto/*.pb.go

# Run a leader node
run-leader: build
	@echo "Running leader node..."
	@mkdir -p data/leader
	./bin/polyfs-node -id leader -addr 127.0.0.1:9000 -data ./data -bootstrap

# Run a follower node
run-follower: build
	@echo "Running follower node..."
	@mkdir -p data/follower1
	./bin/polyfs-node -id follower1 -addr 127.0.0.1:9001 -data ./data -join 127.0.0.1:9000

# Run a client
run-client: build
	@echo "Running client..."
	./bin/polyfs-client -node 127.0.0.1:9000

# Run a 3-node cluster demo
run-demo: build
	@echo "Starting leader node..."
	@mkdir -p data/leader data/follower1 data/follower2
	@echo "Use the following commands in separate terminals:"
	@echo "1. ./bin/polyfs-node -id leader -addr 127.0.0.1:9000 -data ./data -bootstrap"
	@echo "2. ./bin/polyfs-node -id follower1 -addr 127.0.0.1:9001 -data ./data -join 127.0.0.1:9000"
	@echo "3. ./bin/polyfs-node -id follower2 -addr 127.0.0.1:9002 -data ./data -join 127.0.0.1:9000"
	@echo "4. ./bin/polyfs-client -node 127.0.0.1:9000" 