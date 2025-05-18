package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"archive/tar"
	"github.com/hashicorp/raft"
)

// FSM implements the raft.FSM interface for our distributed file system
type FSM struct {
	log     *log.Logger
	dataDir string
	mu      sync.RWMutex
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	var command FSCommand
	if err := json.Unmarshal(logEntry.Data, &command); err != nil {
		f.log.Printf("Failed to unmarshal command: %v", err)
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	f.log.Printf("Applying command: %s on path %s", command.Type, command.Path)

	var response interface{}
	var err error

	switch command.Type {
	case "put":
		err = f.applyPut(command)
	case "delete":
		err = f.applyDelete(command)
	case "mkdir":
		err = f.applyMkdir(command)
	case "rmdir":
		err = f.applyRmdir(command)
	case "join":
		// Node join requests are handled by the cluster management layer
		return nil
	case "leave":
		// Node leave requests are handled by the cluster management layer
		return nil
	default:
		err = fmt.Errorf("unknown command type: %s", command.Type)
	}

	if err != nil {
		f.log.Printf("Error applying command: %v", err)
		return err
	}

	return nil
}

// Snapshot returns a snapshot of the current state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	f.log.Printf("Creating FSM snapshot")

	return &FSMSnapshot{
		dataDir: f.dataDir,
		log:     f.log,
	}, nil
}

// Restore restores the FSM to a previous state
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.log.Printf("Restoring FSM from snapshot")

	// Clean existing data directory
	if err := os.RemoveAll(f.dataDir); err != nil {
		return fmt.Errorf("failed to clean data directory: %v", err)
	}

	if err := os.MkdirAll(f.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Untar the snapshot
	if err := untarDirectory(rc, f.dataDir); err != nil {
		return fmt.Errorf("failed to restore from snapshot: %v", err)
	}

	return nil
}

// applyPut handles a file put command
func (f *FSM) applyPut(cmd FSCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure parent directory exists
	dir := filepath.Dir(filepath.Join(f.dataDir, cmd.Path))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Write file
	if err := os.WriteFile(filepath.Join(f.dataDir, cmd.Path), cmd.Content, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	// Write metadata if provided
	if len(cmd.Attrs) > 0 {
		metaFile := filepath.Join(f.dataDir, cmd.Path+".meta")
		metaData, err := json.Marshal(cmd.Attrs)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}

		if err := os.WriteFile(metaFile, metaData, 0644); err != nil {
			return fmt.Errorf("failed to write metadata: %v", err)
		}
	}

	return nil
}

// applyDelete handles a file delete command
func (f *FSM) applyDelete(cmd FSCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Delete file
	filePath := filepath.Join(f.dataDir, cmd.Path)
	if err := os.Remove(filePath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete file: %v", err)
		}
	}

	// Delete metadata if exists
	metaPath := filepath.Join(f.dataDir, cmd.Path+".meta")
	if err := os.Remove(metaPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete metadata: %v", err)
		}
	}

	return nil
}

// applyMkdir handles a directory creation command
func (f *FSM) applyMkdir(cmd FSCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Create directory
	dirPath := filepath.Join(f.dataDir, cmd.Path)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Write metadata if provided
	if len(cmd.Attrs) > 0 {
		metaFile := filepath.Join(f.dataDir, cmd.Path, ".meta")
		metaData, err := json.Marshal(cmd.Attrs)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}

		if err := os.WriteFile(metaFile, metaData, 0644); err != nil {
			return fmt.Errorf("failed to write metadata: %v", err)
		}
	}

	return nil
}

// applyRmdir handles a directory removal command
func (f *FSM) applyRmdir(cmd FSCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	dirPath := filepath.Join(f.dataDir, cmd.Path)

	// Check if path exists
	info, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, nothing to do
		}
		return fmt.Errorf("failed to access directory: %v", err)
	}

	// Ensure it's a directory
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", cmd.Path)
	}

	// Remove directory
	if cmd.Recursive {
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("failed to remove directory recursively: %v", err)
		}
	} else {
		if err := os.Remove(dirPath); err != nil {
			return fmt.Errorf("failed to remove directory: %v", err)
		}
	}

	return nil
}

// FSMSnapshot implements the raft.FSMSnapshot interface
type FSMSnapshot struct {
	dataDir string
	log     *log.Logger
}

// Persist writes the snapshot to the given sink
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	f.log.Printf("Persisting FSM snapshot to %s", sink.ID())

	// Create a temporary TAR file
	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("polyfs-snapshot-%d.tar", time.Now().UnixNano()))
	if err := tarDirectory(f.dataDir, tmpFile); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to create tar: %v", err)
	}
	defer os.Remove(tmpFile)

	// Open the tar file
	file, err := os.Open(tmpFile)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to open tar: %v", err)
	}
	defer file.Close()

	// Copy the tar to the sink
	_, err = io.Copy(sink, file)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write to sink: %v", err)
	}

	return sink.Close()
}

// Release is called when we are finished with the snapshot
func (f *FSMSnapshot) Release() {}

// tarDirectory creates a tar archive of the specified directory
func tarDirectory(sourceDir, destFile string) error {
	// Create the tar file
	tarFile, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create tar file: %v", err)
	}
	defer tarFile.Close()

	// Create a new tar writer
	tw := tar.NewWriter(tarFile)
	defer tw.Close()

	// Walk through the directory
	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory
		if path == sourceDir {
			return nil
		}

		// Create header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("failed to create tar header: %v", err)
		}

		// Adjust the header name to be relative to the source directory
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %v", err)
		}
		header.Name = relPath

		// Write the header
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header: %v", err)
		}

		// If it's a regular file, write its contents
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open file: %v", err)
			}
			defer file.Close()

			if _, err := io.Copy(tw, file); err != nil {
				return fmt.Errorf("failed to write file contents to tar: %v", err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %v", err)
	}

	return nil
}

// untarDirectory extracts a tar archive to the specified directory
func untarDirectory(src io.Reader, destDir string) error {
	// Create a new tar reader
	tr := tar.NewReader(src)

	// Extract each file
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %v", err)
		}

		// Create the full path
		target := filepath.Join(destDir, header.Name)

		// Check for path traversal
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal path traversal: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(target, 0755); err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}

		case tar.TypeReg:
			// Create parent directories
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("failed to create parent directories: %v", err)
			}

			// Create the file
			file, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file: %v", err)
			}

			// Copy file contents
			if _, err := io.Copy(file, tr); err != nil {
				file.Close()
				return fmt.Errorf("failed to write file contents: %v", err)
			}
			file.Close()

		default:
			return fmt.Errorf("unknown tar type: %v", header.Typeflag)
		}
	}

	return nil
}
