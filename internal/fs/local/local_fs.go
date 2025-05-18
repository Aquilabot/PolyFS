package local

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aquilabot/PolyFS/internal/fs"
)

// LocalFS implements the fs.FileSystem interface using local storage
type LocalFS struct {
	rootDir string
	metaDir string
	mu      sync.RWMutex
}

// NewLocalFS creates a new local file system backend
func NewLocalFS(rootDir string) (*LocalFS, error) {
	// Ensure required directories exist
	metaDir := filepath.Join(rootDir, ".metadata")

	for _, dir := range []string{rootDir, metaDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	return &LocalFS{
		rootDir: rootDir,
		metaDir: metaDir,
	}, nil
}

// dataPath returns the path where file data is stored
func (l *LocalFS) dataPath(path string) string {
	return filepath.Join(l.rootDir, path)
}

// metaPath returns the path where file metadata is stored
func (l *LocalFS) metaPath(path string) string {
	return filepath.Join(l.metaDir, path+".meta.json")
}

// Put stores a file in the file system
func (l *LocalFS) Put(path string, reader io.Reader, attrs map[string]string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Ensure directory exists
	dir := filepath.Dir(l.dataPath(path))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to create directory: " + err.Error(),
		}
	}

	// Create file
	file, err := os.Create(l.dataPath(path))
	if err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to create file: " + err.Error(),
		}
	}
	defer file.Close()

	// Copy data to file
	written, err := io.Copy(file, reader)
	if err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to write data: " + err.Error(),
		}
	}

	// Create and store metadata
	info := fs.FileInfo{
		Path:      path,
		FileSize:  uint64(written),
		Type:      fs.FileRegular,
		Timestamp: time.Now(),
		Attrs:     attrs,
	}

	return l.saveMetadata(path, &info)
}

// Get retrieves a file from the file system
func (l *LocalFS) Get(path string) (io.ReadCloser, *fs.FileInfo, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	info, err := l.getMetadata(path)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.Open(l.dataPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, &fs.FileSystemError{
				Code:    fs.ErrNotFound,
				Message: "file not found: " + path,
			}
		}
		return nil, nil, &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to open file: " + err.Error(),
		}
	}

	return file, info, nil
}

// Delete removes a file from the file system
func (l *LocalFS) Delete(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// First check if file exists
	if _, err := os.Stat(l.dataPath(path)); err != nil {
		if os.IsNotExist(err) {
			return &fs.FileSystemError{
				Code:    fs.ErrNotFound,
				Message: "file not found: " + path,
			}
		}
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to stat file: " + err.Error(),
		}
	}

	// Remove data file and metadata
	if err := os.Remove(l.dataPath(path)); err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to delete file: " + err.Error(),
		}
	}

	// Try to remove metadata, but don't fail if it doesn't exist
	_ = os.Remove(l.metaPath(path))

	return nil
}

// MakeDir creates a directory
func (l *LocalFS) MakeDir(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	dirPath := l.dataPath(path)

	// Check if directory already exists
	info, err := os.Stat(dirPath)
	if err == nil {
		if info.IsDir() {
			return &fs.FileSystemError{
				Code:    fs.ErrAlreadyExists,
				Message: "directory already exists: " + path,
			}
		}
		return &fs.FileSystemError{
			Code:    fs.ErrAlreadyExists,
			Message: "a file with this name already exists: " + path,
		}
	} else if !os.IsNotExist(err) {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to stat directory: " + err.Error(),
		}
	}

	// Create directory
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to create directory: " + err.Error(),
		}
	}

	// Create and store directory metadata
	fileInfo := fs.FromOSFileInfo(path, info)
	return l.saveMetadata(path, &fileInfo)
}

// RemoveDir removes a directory
func (l *LocalFS) RemoveDir(path string, recursive bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	dirPath := l.dataPath(path)

	// Check if directory exists
	info, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &fs.FileSystemError{
				Code:    fs.ErrNotFound,
				Message: "directory not found: " + path,
			}
		}
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to stat directory: " + err.Error(),
		}
	}

	if !info.IsDir() {
		return &fs.FileSystemError{
			Code:    fs.ErrInvalidArgument,
			Message: "not a directory: " + path,
		}
	}

	// Remove directory and metadata
	var removeErr error
	if recursive {
		removeErr = os.RemoveAll(dirPath)
	} else {
		removeErr = os.Remove(dirPath)
	}

	if removeErr != nil {
		if os.IsNotExist(removeErr) {
			return &fs.FileSystemError{
				Code:    fs.ErrNotFound,
				Message: "directory not found: " + path,
			}
		} else if os.IsPermission(removeErr) {
			return &fs.FileSystemError{
				Code:    fs.ErrPermissionDenied,
				Message: "permission denied: " + removeErr.Error(),
			}
		}
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to remove directory: " + removeErr.Error(),
		}
	}

	// Try to remove metadata, but don't fail if it doesn't exist
	_ = os.Remove(l.metaPath(path))

	return nil
}

// List returns a list of files in a directory
func (l *LocalFS) List(dir string, recursive bool) ([]fs.FileInfo, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	dirPath := l.dataPath(dir)

	// Check if directory exists
	info, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &fs.FileSystemError{
				Code:    fs.ErrNotFound,
				Message: "directory not found: " + dir,
			}
		}
		return nil, &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to stat directory: " + err.Error(),
		}
	}

	if !info.IsDir() {
		return nil, &fs.FileSystemError{
			Code:    fs.ErrInvalidArgument,
			Message: "not a directory: " + dir,
		}
	}

	// List files
	var result []fs.FileInfo
	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the metadata directory
		if filepath.HasPrefix(path, l.metaDir) {
			return filepath.SkipDir
		}

		// Skip the root directory itself
		if path == dirPath {
			return nil
		}

		// Get relative path from root dir
		relPath, err := filepath.Rel(l.rootDir, path)
		if err != nil {
			return err
		}

		// Get file metadata if available
		fileInfo, err := l.getMetadata(relPath)
		if err != nil {
			// If metadata not found, create a basic FileInfo
			convertedInfo := fs.FromOSFileInfo(relPath, info)
			fileInfo = &convertedInfo
		}

		result = append(result, *fileInfo)

		// Skip directory traversal if not recursive
		if !recursive && info.IsDir() && path != dirPath {
			return filepath.SkipDir
		}

		return nil
	}

	if err := filepath.Walk(dirPath, walkFn); err != nil {
		return nil, &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to list directory: " + err.Error(),
		}
	}

	return result, nil
}

// saveMetadata stores file metadata
func (l *LocalFS) saveMetadata(path string, info *fs.FileInfo) error {
	// Create directory for metadata if it doesn't exist
	metaDir := filepath.Dir(l.metaPath(path))
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to create metadata directory: " + err.Error(),
		}
	}

	// Marshal metadata to JSON
	data, err := json.Marshal(info)
	if err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to marshal metadata: " + err.Error(),
		}
	}

	// Write metadata to file
	if err := os.WriteFile(l.metaPath(path), data, 0644); err != nil {
		return &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to write metadata: " + err.Error(),
		}
	}

	return nil
}

// getMetadata retrieves file metadata
func (l *LocalFS) getMetadata(path string) (*fs.FileInfo, error) {
	data, err := os.ReadFile(l.metaPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			// Check if the data file exists
			if _, err := os.Stat(l.dataPath(path)); err != nil {
				if os.IsNotExist(err) {
					return nil, &fs.FileSystemError{
						Code:    fs.ErrNotFound,
						Message: "file not found: " + path,
					}
				}
			}

			// Data file exists, but metadata is missing
			// Create a basic FileInfo from file stats
			info, err := os.Stat(l.dataPath(path))
			if err != nil {
				return nil, &fs.FileSystemError{
					Code:    fs.ErrInternalError,
					Message: "failed to stat file: " + err.Error(),
				}
			}

			fileInfo := fs.FromOSFileInfo(path, info)
			return &fileInfo, nil
		}
		return nil, &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to read metadata: " + err.Error(),
		}
	}

	var info fs.FileInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, &fs.FileSystemError{
			Code:    fs.ErrInternalError,
			Message: "failed to unmarshal metadata: " + err.Error(),
		}
	}

	return &info, nil
}
