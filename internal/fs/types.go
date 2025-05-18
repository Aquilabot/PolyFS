package fs

import (
	"io"
	"time"
)

// FileMode represents the file permissions and type
type FileMode uint32

const (
	// FileRegular is a regular file
	FileRegular FileMode = 1 << iota
	// FileDirectory is a directory
	FileDirectory
)

// FileInfo contains metadata about a file
type FileInfo struct {
	Path      string
	Size      uint64
	Mode      FileMode
	Timestamp time.Time
	Attrs     map[string]string
}

// FileSystem defines the interface for file operations
type FileSystem interface {
	// File operations
	Put(path string, reader io.Reader, attrs map[string]string) error
	Get(path string) (io.ReadCloser, *FileInfo, error)
	Delete(path string) error
	
	// Directory operations
	MakeDir(path string) error
	RemoveDir(path string, recursive bool) error
	List(dir string, recursive bool) ([]FileInfo, error)
}

// FileChunk represents part of a file for streaming
type FileChunk struct {
	Data      []byte
	IsLastChunk bool
}

// FileSystemError represents an error in the file system operations
type FileSystemError struct {
	Code    ErrorCode
	Message string
}

// ErrorCode represents specific error conditions
type ErrorCode int

const (
	// ErrNotFound is returned when a file does not exist
	ErrNotFound ErrorCode = iota
	// ErrAlreadyExists is returned when trying to create a file that already exists
	ErrAlreadyExists
	// ErrPermissionDenied is returned when access is denied
	ErrPermissionDenied
	// ErrInternalError is returned for internal errors
	ErrInternalError
	// ErrInvalidArgument is returned for invalid parameters
	ErrInvalidArgument
)

// Error implements the error interface
func (e *FileSystemError) Error() string {
	return e.Message
} 