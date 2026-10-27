package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// Storage is where a Node saves its persistent state (currentTerm, votedFor
// and the log; see PersistentState in types.go). It's an interface so
// tests can use an in-memory implementation, and so the node logic never
// depends on files directly.
//
// Save must not return until the state is durable: once Save returns nil,
// the state must survive the process crashing or the machine losing power.
// The Raft paper's Figure 2 relies on that: a node persists its state
// BEFORE responding to any RPC, so it can never, after a restart, forget a
// vote it already told a candidate about, or an entry it already told the
// leader it stored.
type Storage interface {
	Save(state PersistentState) error
}

// stateFileName is the file FileStorage keeps the state in, inside its
// directory. stateFileVersion is written into it, so a future change of
// format can be detected instead of misread.
const (
	stateFileName    = "raft-state.json"
	stateFileVersion = 1
)

// stateFile is the on-disk format: JSON, so the file can be opened and read
// by a person while debugging. Command bytes appear base64-encoded, which
// is how encoding/json writes []byte.
type stateFile struct {
	Version     int        `json:"version"`
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    int        `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

// FileStorage saves the state as one JSON file in a directory, one
// directory per node.
//
// Every Save rewrites the whole state. That's simple and fine for this
// project's log sizes, but it costs O(log length) per write. Production
// systems append only the new entries to a write-ahead log and compact it
// with snapshots instead (listed under Known Limitations in the README).
type FileStorage struct {
	dir string
}

// NewFileStorage returns a FileStorage that keeps its file in dir,
// creating the directory if it doesn't exist.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating data directory %s: %w", dir, err)
	}
	return &FileStorage{dir: dir}, nil
}

// Path is the full path of the state file.
func (s *FileStorage) Path() string { return filepath.Join(s.dir, stateFileName) }

// Save writes the state atomically and durably:
//
//  1. write it to a temporary file next to the real one,
//  2. fsync the temporary file, so its bytes are actually on disk,
//  3. rename it over the real file, and
//  4. fsync the directory, so the rename itself is on disk.
//
// A rename within one directory is atomic, so a crash at any point leaves
// either the complete old state or the complete new one — never a
// half-written file. Writing the real file in place instead could leave it
// truncated or mixed if the process died mid-write, and the node would
// restart with a corrupt or empty log.
func (s *FileStorage) Save(state PersistentState) error {
	data, err := json.Marshal(stateFile{
		Version:     stateFileVersion,
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         state.Log,
	})
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}

	tmpPath := s.Path() + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmpPath, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing %s: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("syncing %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", tmpPath, err)
	}

	// os.Rename replaces an existing file on every platform Go supports,
	// including Windows (where it uses MoveFileEx with REPLACE_EXISTING).
	if err := os.Rename(tmpPath, s.Path()); err != nil {
		return fmt.Errorf("renaming %s over %s: %w", tmpPath, s.Path(), err)
	}
	return syncDir(s.dir)
}

// syncDir fsyncs a directory, making a rename inside it durable. Windows
// can't open a directory for syncing (there, NTFS journaling covers the
// rename's metadata), so this is a no-op on Windows.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening %s to sync it: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("syncing directory %s: %w", dir, err)
	}
	return nil
}