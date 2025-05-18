package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aquilabot/PolyFS/internal/cluster"
	"github.com/aquilabot/PolyFS/internal/fs"
	pb "github.com/aquilabot/PolyFS/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the gRPC interface for our distributed filesystem
type GRPCServer struct {
	pb.UnimplementedFileSystemServer
	pb.UnimplementedClusterManagementServer

	node    *cluster.Node
	fs      fs.FileSystem
	logger  *log.Logger
	chunkSize int
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(node *cluster.Node, filesystem fs.FileSystem, logger *log.Logger) *GRPCServer {
	return &GRPCServer{
		node:      node,
		fs:        filesystem,
		logger:    logger,
		chunkSize: 1024 * 1024, // 1MB chunks for file transfer
	}
}

// PutFile handles file upload requests
func (s *GRPCServer) PutFile(stream pb.FileSystem_PutFileServer) error {
	var metadata *pb.FileMetadata
	var fileBuffer bytes.Buffer
	var attrs map[string]string

	s.logger.Println("Starting file upload")

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive data: %v", err)
		}

		switch data := req.Data.(type) {
		case *pb.PutFileRequest_Metadata:
			if metadata != nil {
				return status.Errorf(codes.InvalidArgument, "metadata already received")
			}
			metadata = data.Metadata
			attrs = metadata.Attributes
			s.logger.Printf("Received metadata for %s, size: %d bytes", metadata.Path, metadata.Size)

		case *pb.PutFileRequest_Chunk:
			if metadata == nil {
				return status.Errorf(codes.InvalidArgument, "received file chunk before metadata")
			}
			fileBuffer.Write(data.Chunk)
			s.logger.Printf("Received chunk of %d bytes", len(data.Chunk))
		}
	}

	if metadata == nil {
		return status.Errorf(codes.InvalidArgument, "no metadata received")
	}

	// Apply the operation through the cluster
	cmd := cluster.FSCommand{
		Type:    "put",
		Path:    metadata.Path,
		Content: fileBuffer.Bytes(),
		Attrs:   attrs,
	}

	if err := s.node.ApplyCommand(cmd); err != nil {
		return status.Errorf(codes.Internal, "failed to apply command: %v", err)
	}

	return stream.SendAndClose(&pb.PutFileResponse{
		Id:      metadata.Path, // Using path as ID for now
		Success: true,
		Message: "File uploaded successfully",
	})
}

// GetFile handles file download requests
func (s *GRPCServer) GetFile(req *pb.GetFileRequest, stream pb.FileSystem_GetFileServer) error {
	s.logger.Printf("Get file request for %s", req.Path)

	file, info, err := s.fs.Get(req.Path)
	if err != nil {
		var code codes.Code
		var message string

		if fsErr, ok := err.(*fs.FileSystemError); ok {
			switch fsErr.Code {
			case fs.ErrNotFound:
				code = codes.NotFound
				message = fmt.Sprintf("file not found: %s", req.Path)
			case fs.ErrPermissionDenied:
				code = codes.PermissionDenied
				message = "permission denied"
			default:
				code = codes.Internal
				message = fmt.Sprintf("internal error: %v", err)
			}
		} else {
			code = codes.Internal
			message = fmt.Sprintf("unknown error: %v", err)
		}

		return status.Errorf(code, message)
	}
	defer file.Close()

	buffer := make([]byte, s.chunkSize)
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "error reading file: %v", err)
		}

		if err := stream.Send(&pb.GetFileResponse{
			Chunk:      buffer[:n],
			IsLastChunk: false,
		}); err != nil {
			return status.Errorf(codes.Internal, "error sending chunk: %v", err)
		}
	}

	// Send the final chunk marker
	return stream.Send(&pb.GetFileResponse{
		Chunk:      []byte{},
		IsLastChunk: true,
	})
}

// DeleteFile handles file deletion requests
func (s *GRPCServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	s.logger.Printf("Delete file request for %s", req.Path)

	cmd := cluster.FSCommand{
		Type: "delete",
		Path: req.Path,
	}

	if err := s.node.ApplyCommand(cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", err)
	}

	return &pb.DeleteFileResponse{
		Success: true,
		Message: "File deleted successfully",
	}, nil
}

// ListFiles handles directory listing requests
func (s *GRPCServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	s.logger.Printf("List files request for %s (recursive: %v)", req.Directory, req.Recursive)

	files, err := s.fs.List(req.Directory, req.Recursive)
	if err != nil {
		var code codes.Code
		var message string

		if fsErr, ok := err.(*fs.FileSystemError); ok {
			switch fsErr.Code {
			case fs.ErrNotFound:
				code = codes.NotFound
				message = fmt.Sprintf("directory not found: %s", req.Directory)
			case fs.ErrPermissionDenied:
				code = codes.PermissionDenied
				message = "permission denied"
			default:
				code = codes.Internal
				message = fmt.Sprintf("internal error: %v", err)
			}
		} else {
			code = codes.Internal
			message = fmt.Sprintf("unknown error: %v", err)
		}

		return nil, status.Errorf(code, message)
	}

	response := &pb.ListFilesResponse{
		Files: make([]*pb.FileInfo, 0, len(files)),
	}

	for _, file := range files {
		info := &pb.FileInfo{
			Path:       file.Path,
			IsDirectory: file.Mode == fs.FileDirectory,
			Size:       file.Size,
			Timestamp:  file.Timestamp.Unix(),
			Attributes: file.Attrs,
		}
		response.Files = append(response.Files, info)
	}

	return response, nil
}

// MakeDirectory handles directory creation requests
func (s *GRPCServer) MakeDirectory(ctx context.Context, req *pb.MakeDirectoryRequest) (*pb.MakeDirectoryResponse, error) {
	s.logger.Printf("Make directory request for %s", req.Path)

	cmd := cluster.FSCommand{
		Type: "mkdir",
		Path: req.Path,
	}

	if err := s.node.ApplyCommand(cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", err)
	}

	return &pb.MakeDirectoryResponse{
		Success: true,
		Message: "Directory created successfully",
	}, nil
}

// RemoveDirectory handles directory removal requests
func (s *GRPCServer) RemoveDirectory(ctx context.Context, req *pb.RemoveDirectoryRequest) (*pb.RemoveDirectoryResponse, error) {
	s.logger.Printf("Remove directory request for %s (recursive: %v)", req.Path, req.Recursive)

	cmd := cluster.FSCommand{
		Type:     "rmdir",
		Path:     req.Path,
		Recursive: req.Recursive,
	}

	if err := s.node.ApplyCommand(cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", err)
	}

	return &pb.RemoveDirectoryResponse{
		Success: true,
		Message: "Directory removed successfully",
	}, nil
}

// JoinCluster handles node join requests
func (s *GRPCServer) JoinCluster(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	s.logger.Printf("Join cluster request from %s at %s", req.NodeId, req.Address)

	if err := s.node.AddNode(req.NodeId, req.Address); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add node: %v", err)
	}

	nodes := s.node.GetNodes()
	pbNodes := make([]*pb.NodeInfo, 0, len(nodes))

	for _, node := range nodes {
		var state pb.NodeState
		switch node.State {
		case cluster.NodeStateHealthy:
			state = pb.NodeState_HEALTHY
		case cluster.NodeStateSyncing:
			state = pb.NodeState_SYNCING
		case cluster.NodeStateDegraded:
			state = pb.NodeState_DEGRADED
		case cluster.NodeStateOffline:
			state = pb.NodeState_OFFLINE
		default:
			state = pb.NodeState_UNKNOWN
		}

		pbNodes = append(pbNodes, &pb.NodeInfo{
			NodeId:   node.ID,
			Address:  node.Address,
			State:    state,
			IsLeader: node.IsLeader,
		})
	}

	return &pb.JoinResponse{
		Success:  true,
		ClusterId: "polyfs-cluster", // Hard-coded for now
		LeaderId: "", // TODO: Get the leader ID
		Message:  "Successfully joined cluster",
		Nodes:    pbNodes,
	}, nil
}

// LeaveCluster handles node leave requests
func (s *GRPCServer) LeaveCluster(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	s.logger.Printf("Leave cluster request from %s", req.NodeId)

	if err := s.node.RemoveNode(req.NodeId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove node: %v", err)
	}

	return &pb.LeaveResponse{
		Success: true,
		Message: "Successfully left cluster",
	}, nil
}

// GetClusterStatus handles status requests
func (s *GRPCServer) GetClusterStatus(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	s.logger.Printf("Get cluster status request (include metrics: %v)", req.IncludeMetrics)

	nodes := s.node.GetNodes()
	pbNodes := make([]*pb.NodeInfo, 0, len(nodes))

	for _, node := range nodes {
		var state pb.NodeState
		switch node.State {
		case cluster.NodeStateHealthy:
			state = pb.NodeState_HEALTHY
		case cluster.NodeStateSyncing:
			state = pb.NodeState_SYNCING
		case cluster.NodeStateDegraded:
			state = pb.NodeState_DEGRADED
		case cluster.NodeStateOffline:
			state = pb.NodeState_OFFLINE
		default:
			state = pb.NodeState_UNKNOWN
		}

		pbNodes = append(pbNodes, &pb.NodeInfo{
			NodeId:   node.ID,
			Address:  node.Address,
			State:    state,
			IsLeader: node.IsLeader,
		})
	}

	var metrics *pb.ClusterMetrics
	if req.IncludeMetrics {
		metrics = &pb.ClusterMetrics{
			TotalStorage:   0, // TODO: Implement storage metrics
			UsedStorage:    0,
			OperationCount: 0,
		}
	}

	return &pb.StatusResponse{
		ClusterId: "polyfs-cluster", // Hard-coded for now
		LeaderId:  "", // TODO: Get the leader ID
		NodeCount: int32(len(nodes)),
		Nodes:     pbNodes,
		Metrics:   metrics,
	}, nil
} 