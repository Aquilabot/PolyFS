package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// NodeState represents the state of a node in the cluster
type NodeState int

const (
	NodeStateUnknown NodeState = iota
	NodeStateHealthy
	NodeStateSyncing
	NodeStateDegraded
	NodeStateOffline
)

// NodeInfo contains information about a node
type NodeInfo struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	State    NodeState `json:"state"`
	IsLeader bool      `json:"is_leader"`
}

// FSCommand represents a file system operation to be replicated
type FSCommand struct {
	Type      string            `json:"type"`
	Path      string            `json:"path"`
	Content   []byte            `json:"content,omitempty"`
	Recursive bool              `json:"recursive,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
}

// ClusterMetrics contains metrics about the cluster
type ClusterMetrics struct {
	TotalStorage   int64 `json:"total_storage"`
	UsedStorage    int64 `json:"used_storage"`
	OperationCount int64 `json:"operation_count"`
}

// Node represents a node in the cluster
type Node struct {
	ID        string
	Address   string
	DataDir   string
	raftDir   string
	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport
	log       *log.Logger
	metrics   ClusterMetrics
	nodes     map[string]*NodeInfo
	nodesMu   sync.RWMutex
}

// NewNode creates a new node
func NewNode(id, address, dataDir string) (*Node, error) {
	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %v", err)
	}

	node := &Node{
		ID:      id,
		Address: address,
		DataDir: dataDir,
		raftDir: raftDir,
		log:     log.New(os.Stderr, fmt.Sprintf("[NODE %s] ", id), log.LstdFlags),
		metrics: ClusterMetrics{},
		nodes:   make(map[string]*NodeInfo),
	}

	return node, nil
}

// Start initializes the Raft server
func (n *Node) Start() error {
	// Create Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.ID)
	
	// Setup Raft communication
	addr, err := net.ResolveTCPAddr("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %v", err)
	}
	
	transport, err := raft.NewTCPTransport(n.Address, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create TCP transport: %v", err)
	}
	n.transport = transport
	
	// Create the snapshot store
	snapshots, err := raft.NewFileSnapshotStore(n.raftDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %v", err)
	}
	
	// Create the log store and stable store
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(n.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("failed to create BoltDB store: %v", err)
	}
	
	// Create the finite state machine
	n.fsm = &FSM{
		log:    n.log,
		dataDir: filepath.Join(n.DataDir, "data"),
	}
	
	// Create Raft instance
	ra, err := raft.NewRaft(config, n.fsm, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create Raft instance: %v", err)
	}
	n.raft = ra
	
	n.log.Printf("Node %s started at %s", n.ID, n.Address)
	
	// Register ourselves in the node list
	n.nodesMu.Lock()
	n.nodes[n.ID] = &NodeInfo{
		ID:       n.ID,
		Address:  n.Address,
		State:    NodeStateHealthy,
		IsLeader: false, // Will be updated in the background
	}
	n.nodesMu.Unlock()
	
	// Start a goroutine to monitor leadership status
	go n.monitorLeadership()
	
	return nil
}

// monitorLeadership periodically checks if this node is the leader and updates node status
func (n *Node) monitorLeadership() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		isLeader := n.raft.State() == raft.Leader
		
		n.nodesMu.Lock()
		if info, ok := n.nodes[n.ID]; ok {
			info.IsLeader = isLeader
		}
		n.nodesMu.Unlock()
		
		if isLeader {
			// If we're the leader, update our knowledge of the cluster
			future := n.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				n.log.Printf("Failed to get configuration: %v", err)
				continue
			}
			
			servers := future.Configuration().Servers
			
			// Update node list based on Raft configuration
			n.nodesMu.Lock()
			for _, server := range servers {
				id := string(server.ID)
				addr := string(server.Address)
				
				if _, exists := n.nodes[id]; !exists {
					n.nodes[id] = &NodeInfo{
						ID:       id,
						Address:  addr,
						State:    NodeStateHealthy,
						IsLeader: id == n.ID && isLeader,
					}
				}
			}
			n.nodesMu.Unlock()
		}
	}
}

// Bootstrap bootstraps the Raft cluster (for the first node)
func (n *Node) Bootstrap() error {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.ID),
				Address: raft.ServerAddress(n.Address),
			},
		},
	}
	
	return n.raft.BootstrapCluster(configuration).Error()
}

// Join joins an existing Raft cluster
func (n *Node) Join(leaderAddr string) error {
	n.log.Printf("Joining cluster via leader at %s", leaderAddr)
	
	// Connect to the leader
	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %v", err)
	}
	defer conn.Close()
	
	// Send join request
	cmd := FSCommand{
		Type: "join",
		Path: n.ID,
		Attrs: map[string]string{
			"address": n.Address,
		},
	}
	
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal join request: %v", err)
	}
	
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to send join request: %v", err)
	}
	
	// Read response
	resp := make([]byte, 1024)
	n, err := conn.Read(resp)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("connection closed unexpectedly")
		}
		return fmt.Errorf("failed to read response: %v", err)
	}
	
	var response struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	
	if err := json.Unmarshal(resp[:n], &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}
	
	if !response.Success {
		return fmt.Errorf("join request failed: %s", response.Message)
	}
	
	return nil
}

// AddNode adds a new node to the cluster
func (n *Node) AddNode(id, addr string) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}
	
	n.log.Printf("Adding node %s at %s to cluster", id, addr)
	
	future := n.raft.AddVoter(
		raft.ServerID(id),
		raft.ServerAddress(addr),
		0,
		10*time.Second,
	)
	
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %v", err)
	}
	
	// Add node to our list
	n.nodesMu.Lock()
	n.nodes[id] = &NodeInfo{
		ID:       id,
		Address:  addr,
		State:    NodeStateHealthy,
		IsLeader: false,
	}
	n.nodesMu.Unlock()
	
	return nil
}

// RemoveNode removes a node from the cluster
func (n *Node) RemoveNode(id string) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}
	
	n.log.Printf("Removing node %s from cluster", id)
	
	future := n.raft.RemoveServer(
		raft.ServerID(id),
		0,
		10*time.Second,
	)
	
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %v", err)
	}
	
	// Remove node from our list
	n.nodesMu.Lock()
	delete(n.nodes, id)
	n.nodesMu.Unlock()
	
	return nil
}

// GetNodes returns the list of nodes in the cluster
func (n *Node) GetNodes() []*NodeInfo {
	n.nodesMu.RLock()
	defer n.nodesMu.RUnlock()
	
	var nodes []*NodeInfo
	for _, node := range n.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	
	return nodes
}

// ApplyCommand applies a command to the Raft log
func (n *Node) ApplyCommand(cmd FSCommand) error {
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}
	
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %v", err)
	}
	
	future := n.raft.Apply(data, 10*time.Second)
	return future.Error()
}

// Shutdown closes the Raft server
func (n *Node) Shutdown() error {
	n.log.Printf("Shutting down node %s", n.ID)
	
	if n.raft != nil {
		future := n.raft.Shutdown()
		if err := future.Error(); err != nil {
			return fmt.Errorf("failed to shutdown Raft: %v", err)
		}
	}
	
	if n.transport != nil {
		if err := n.transport.Close(); err != nil {
			return fmt.Errorf("failed to close transport: %v", err)
		}
	}
	
	return nil
} 