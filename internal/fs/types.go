package fs

import (
	"io"
	"os"
	"path/filepath"
	"time"
)

// FileType represents the type of file
type FileType uint32

const (
	// FileRegular is a regular file
	FileRegular FileType = 1 << iota
	// FileDirectory is a directory
	FileDirectory
)

// FileInfo contains metadata about a file and implements os.FileInfo
type FileInfo struct {
	Path      string
	FileSize  uint64
	Type      FileType
	Timestamp time.Time
	Attrs     map[string]string
}

// Name returns the base name of the file
func (f FileInfo) Name() string {
	return filepath.Base(f.Path)
}

// Size returns the file size
func (f FileInfo) Size() int64 {
	return int64(f.FileSize)
}

// Mode returns the file mode
func (f FileInfo) Mode() os.FileMode {
	if f.Type == FileDirectory {
		return os.ModeDir | 0755
	}
	return 0644
}

// ModTime returns the file's modification time
func (f FileInfo) ModTime() time.Time {
	return f.Timestamp
}

// IsDir returns whether the file is a directory
func (f FileInfo) IsDir() bool {
	return f.Type == FileDirectory
}

// Sys returns nil as we don't need system-specific information
func (f FileInfo) Sys() interface{} {
	return nil
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
	Data        []byte
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

// FromOSFileInfo converts an os.FileInfo to our FileInfo type
func FromOSFileInfo(path string, info os.FileInfo) FileInfo {
	fileType := FileRegular
	if info.IsDir() {
		fileType = FileDirectory
	}
	
	return FileInfo{
		Path:      path,
		FileSize:  uint64(info.Size()),
		Type:      fileType,
		Timestamp: info.ModTime(),
		Attrs:     make(map[string]string),
	}
}
