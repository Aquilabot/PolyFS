package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/aquilabot/PolyFS/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	nodeAddr = flag.String("node", "127.0.0.1:9000", "Address of a node to connect to")
)

// Client is a simple client for the distributed file system
type Client struct {
	fsClient     pb.FileSystemClient
	clusterClient pb.ClusterManagementClient
	logger       *log.Logger
}

func main() {
	flag.Parse()

	// Set up logger
	logger := log.New(os.Stdout, "[CLIENT] ", log.LstdFlags)

	// Connect to server
	logger.Printf("Connecting to node at %s", *nodeAddr)
	conn, err := grpc.Dial(*nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create client
	client := &Client{
		fsClient:      pb.NewFileSystemClient(conn),
		clusterClient: pb.NewClusterManagementClient(conn),
		logger:        logger,
	}

	fmt.Println("PolyFS Client")
	fmt.Println("Type 'help' for available commands")

	// Start interactive shell
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("polyfs> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		command := args[0]

		switch command {
		case "help":
			client.printHelp()
		case "put":
			if len(args) < 3 {
				fmt.Println("Usage: put <local_file> <remote_path>")
				continue
			}
			client.putFile(args[1], args[2])
		case "get":
			if len(args) < 3 {
				fmt.Println("Usage: get <remote_path> <local_file>")
				continue
			}
			client.getFile(args[1], args[2])
		case "delete", "rm":
			if len(args) < 2 {
				fmt.Println("Usage: delete <remote_path>")
				continue
			}
			client.deleteFile(args[1])
		case "list", "ls":
			recursive := false
			path := "/"
			if len(args) > 1 {
				if args[1] == "-r" || args[1] == "--recursive" {
					recursive = true
					if len(args) > 2 {
						path = args[2]
					}
				} else {
					path = args[1]
					if len(args) > 2 && (args[2] == "-r" || args[2] == "--recursive") {
						recursive = true
					}
				}
			}
			client.listFiles(path, recursive)
		case "mkdir":
			if len(args) < 2 {
				fmt.Println("Usage: mkdir <remote_path>")
				continue
			}
			client.makeDirectory(args[1])
		case "rmdir":
			recursive := false
			if len(args) < 2 {
				fmt.Println("Usage: rmdir [-r] <remote_path>")
				continue
			}
			path := args[1]
			if path == "-r" || path == "--recursive" {
				if len(args) < 3 {
					fmt.Println("Usage: rmdir -r <remote_path>")
					continue
				}
				recursive = true
				path = args[2]
			}
			client.removeDirectory(path, recursive)
		case "status":
			client.getStatus()
		case "exit", "quit":
			return
		default:
			fmt.Printf("Unknown command: %s\n", command)
			client.printHelp()
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("Error reading input: %v", err)
	}
}

// printHelp prints available commands
func (c *Client) printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  help                      Show this help")
	fmt.Println("  put <local_file> <path>   Upload a file")
	fmt.Println("  get <path> <local_file>   Download a file")
	fmt.Println("  delete|rm <path>          Delete a file")
	fmt.Println("  list|ls [-r] <path>       List files (-r for recursive)")
	fmt.Println("  mkdir <path>              Create a directory")
	fmt.Println("  rmdir [-r] <path>         Remove a directory (-r for recursive)")
	fmt.Println("  status                    Show cluster status")
	fmt.Println("  exit|quit                 Exit the client")
}

// putFile uploads a file to the distributed file system
func (c *Client) putFile(localPath, remotePath string) {
	// Open local file
	file, err := os.Open(localPath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Failed to get file info: %v\n", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create stream
	stream, err := c.fsClient.PutFile(ctx)
	if err != nil {
		fmt.Printf("Failed to create stream: %v\n", err)
		return
	}

	// Send metadata
	if err := stream.Send(&pb.PutFileRequest{
		Data: &pb.PutFileRequest_Metadata{
			Metadata: &pb.FileMetadata{
				Path:       remotePath,
				Size:       uint64(fileInfo.Size()),
				Timestamp:  time.Now().Unix(),
				Attributes: make(map[string]string),
			},
		},
	}); err != nil {
		fmt.Printf("Failed to send metadata: %v\n", err)
		return
	}

	// Send file in chunks
	buffer := make([]byte, 1024*1024) // 1MB chunks
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Failed to read file: %v\n", err)
			return
		}

		if err := stream.Send(&pb.PutFileRequest{
			Data: &pb.PutFileRequest_Chunk{
				Chunk: buffer[:n],
			},
		}); err != nil {
			fmt.Printf("Failed to send chunk: %v\n", err)
			return
		}
	}

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		fmt.Printf("Failed to close stream: %v\n", err)
		return
	}

	if resp.Success {
		fmt.Printf("File uploaded successfully: %s\n", resp.Id)
	} else {
		fmt.Printf("Failed to upload file: %s\n", resp.Message)
	}
}

// getFile downloads a file from the distributed file system
func (c *Client) getFile(remotePath, localPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create request
	req := &pb.GetFileRequest{
		Path: remotePath,
	}

	// Create stream
	stream, err := c.fsClient.GetFile(ctx, req)
	if err != nil {
		fmt.Printf("Failed to create stream: %v\n", err)
		return
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		fmt.Printf("Failed to create directory: %v\n", err)
		return
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		fmt.Printf("Failed to create file: %v\n", err)
		return
	}
	defer file.Close()

	// Receive chunks
	totalBytes := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Failed to receive chunk: %v\n", err)
			return
		}

		if len(resp.Chunk) > 0 {
			n, err := file.Write(resp.Chunk)
			if err != nil {
				fmt.Printf("Failed to write to file: %v\n", err)
				return
			}
			totalBytes += n
		}

		if resp.IsLastChunk {
			break
		}
	}

	fmt.Printf("File downloaded successfully to %s (%d bytes)\n", localPath, totalBytes)
}

// deleteFile deletes a file from the distributed file system
func (c *Client) deleteFile(path string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create request
	req := &pb.DeleteFileRequest{
		Path: path,
	}

	// Send request
	resp, err := c.fsClient.DeleteFile(ctx, req)
	if err != nil {
		fmt.Printf("Failed to delete file: %v\n", err)
		return
	}

	if resp.Success {
		fmt.Printf("File deleted successfully: %s\n", path)
	} else {
		fmt.Printf("Failed to delete file: %s\n", resp.Message)
	}
}

// listFiles lists files in the distributed file system
func (c *Client) listFiles(path string, recursive bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create request
	req := &pb.ListFilesRequest{
		Directory: path,
		Recursive: recursive,
	}

	// Send request
	resp, err := c.fsClient.ListFiles(ctx, req)
	if err != nil {
		fmt.Printf("Failed to list files: %v\n", err)
		return
	}

	// Print files
	fmt.Printf("Contents of %s:\n", path)
	for _, file := range resp.Files {
		fileType := "file"
		if file.IsDirectory {
			fileType = "dir "
		}
		fmt.Printf("%s %10d %s %s\n",
			fileType,
			file.Size,
			time.Unix(file.Timestamp, 0).Format("2006-01-02 15:04:05"),
			file.Path,
		)
	}
	fmt.Printf("Total: %d items\n", len(resp.Files))
}

// makeDirectory creates a directory in the distributed file system
func (c *Client) makeDirectory(path string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create request
	req := &pb.MakeDirectoryRequest{
		Path: path,
	}

	// Send request
	resp, err := c.fsClient.MakeDirectory(ctx, req)
	if err != nil {
		fmt.Printf("Failed to create directory: %v\n", err)
		return
	}

	if resp.Success {
		fmt.Printf("Directory created successfully: %s\n", path)
	} else {
		fmt.Printf("Failed to create directory: %s\n", resp.Message)
	}
}

// removeDirectory removes a directory from the distributed file system
func (c *Client) removeDirectory(path string, recursive bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create request
	req := &pb.RemoveDirectoryRequest{
		Path:      path,
		Recursive: recursive,
	}

	// Send request
	resp, err := c.fsClient.RemoveDirectory(ctx, req)
	if err != nil {
		fmt.Printf("Failed to remove directory: %v\n", err)
		return
	}

	if resp.Success {
		fmt.Printf("Directory removed successfully: %s\n", path)
	} else {
		fmt.Printf("Failed to remove directory: %s\n", resp.Message)
	}
}

// getStatus retrieves and displays the cluster status
func (c *Client) getStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create request
	req := &pb.StatusRequest{
		IncludeMetrics: true,
	}

	// Send request
	resp, err := c.clusterClient.GetClusterStatus(ctx, req)
	if err != nil {
		fmt.Printf("Failed to get cluster status: %v\n", err)
		return
	}

	// Print status
	fmt.Printf("Cluster ID: %s\n", resp.ClusterId)
	fmt.Printf("Leader ID: %s\n", resp.LeaderId)
	fmt.Printf("Number of nodes: %d\n", resp.NodeCount)
	fmt.Printf("\nNodes:\n")
	for _, node := range resp.Nodes {
		status := "Follower"
		if node.IsLeader {
			status = "Leader  "
		}
		state := "Unknown"
		switch node.State {
		case pb.NodeState_HEALTHY:
			state = "Healthy"
		case pb.NodeState_SYNCING:
			state = "Syncing"
		case pb.NodeState_DEGRADED:
			state = "Degraded"
		case pb.NodeState_OFFLINE:
			state = "Offline"
		}
		fmt.Printf("  %s  %-8s  %-8s  %s\n", node.NodeId, status, state, node.Address)
	}

	if resp.Metrics != nil {
		fmt.Printf("\nMetrics:\n")
		fmt.Printf("  Total storage: %d bytes\n", resp.Metrics.TotalStorage)
		fmt.Printf("  Used storage: %d bytes\n", resp.Metrics.UsedStorage)
		fmt.Printf("  Operation count: %d\n", resp.Metrics.OperationCount)
	}
} 