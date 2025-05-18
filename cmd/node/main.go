package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/aquilabot/PolyFS/internal/cluster"
	"github.com/aquilabot/PolyFS/internal/fs/local"
	"github.com/aquilabot/PolyFS/internal/server"
	pb "github.com/aquilabot/PolyFS/proto"
	"google.golang.org/grpc"
)

var (
	nodeID   = flag.String("id", "", "Unique ID of this node")
	nodeAddr = flag.String("addr", "127.0.0.1:9000", "Address for this node")
	dataDir  = flag.String("data", "./data", "Data directory")
	joinAddr = flag.String("join", "", "Address of leader node to join (empty for new cluster)")
	bootstrap = flag.Bool("bootstrap", false, "Whether to bootstrap a new cluster")
)

func main() {
	flag.Parse()

	// Set up logger
	logger := log.New(os.Stdout, fmt.Sprintf("[NODE %s] ", *nodeID), log.LstdFlags)

	// Validate flags
	if *nodeID == "" {
		logger.Fatal("Node ID is required")
	}

	// Create data directory with node-specific subdirectory
	nodeDataDir := filepath.Join(*dataDir, *nodeID)
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		logger.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize local filesystem
	filesystem, err := local.NewLocalFS(filepath.Join(nodeDataDir, "local"))
	if err != nil {
		logger.Fatalf("Failed to initialize filesystem: %v", err)
	}

	// Initialize node
	node, err := cluster.NewNode(*nodeID, *nodeAddr, nodeDataDir)
	if err != nil {
		logger.Fatalf("Failed to create node: %v", err)
	}

	// Start node
	if err := node.Start(); err != nil {
		logger.Fatalf("Failed to start node: %v", err)
	}

	// Bootstrap or join cluster
	if *bootstrap {
		logger.Printf("Bootstrapping new cluster")
		if err := node.Bootstrap(); err != nil {
			logger.Fatalf("Failed to bootstrap cluster: %v", err)
		}
	} else if *joinAddr != "" {
		logger.Printf("Joining cluster at %s", *joinAddr)
		if err := node.Join(*joinAddr); err != nil {
			logger.Fatalf("Failed to join cluster: %v", err)
		}
	} else {
		logger.Fatal("Either -bootstrap or -join is required")
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	polyServer := server.NewGRPCServer(node, filesystem, logger)
	pb.RegisterFileSystemServer(grpcServer, polyServer)
	pb.RegisterClusterManagementServer(grpcServer, polyServer)

	// Start listening
	lis, err := net.Listen("tcp", *nodeAddr)
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}

	// Handle interrupt signal
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		logger.Println("Shutting down...")
		grpcServer.GracefulStop()
		if err := node.Shutdown(); err != nil {
			logger.Fatalf("Error during shutdown: %v", err)
		}
		os.Exit(0)
	}()

	// Start server
	logger.Printf("Node %s listening on %s", *nodeID, *nodeAddr)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve: %v", err)
	}
} 