package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// readStateFile decodes FileStorage's file directly, so these tests check
// what is really on disk, independently of the code that will load it.
func readStateFile(t *testing.T, s *FileStorage) stateFile {
	t.Helper()
	data, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatalf("reading state file: %v", err)
	}
	var f stateFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("state file is not valid JSON: %v\n%s", err, data)
	}
	return f
}

func TestFileStorage_SaveWritesTermVoteAndLog(t *testing.T) {
	s, err := NewFileStorage(filepath.Join(t.TempDir(), "node1")) // dir doesn't exist yet
	if err != nil {
		t.Fatal(err)
	}
	state := PersistentState{
		CurrentTerm: 3,
		VotedFor:    2,
		Log: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("SET a 1")},
			{Term: 3, Index: 2, Command: []byte("SET b \x00 binary-safe")},
		},
	}
	if err := s.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	f := readStateFile(t, s)
	if f.Version != stateFileVersion || f.CurrentTerm != 3 || f.VotedFor != 2 || len(f.Log) != 2 {
		t.Fatalf("file content = %+v", f)
	}
	if string(f.Log[1].Command) != "SET b \x00 binary-safe" || f.Log[1].Term != 3 || f.Log[1].Index != 2 {
		t.Fatalf("entry 2 = %+v", f.Log[1])
	}
}

func TestFileStorage_LaterSaveReplacesEarlierOne(t *testing.T) {
	s, _ := NewFileStorage(t.TempDir())
	s.Save(PersistentState{CurrentTerm: 1, VotedFor: 1, Log: entriesWithTerms(1, 1, 1)})
	s.Save(PersistentState{CurrentTerm: 2, VotedFor: -1, Log: entriesWithTerms(1)})

	f := readStateFile(t, s)
	if f.CurrentTerm != 2 || f.VotedFor != -1 || len(f.Log) != 1 {
		t.Fatalf("file should hold only the latest state, got %+v", f)
	}
	if _, err := os.Stat(s.Path() + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind after a successful save (err=%v)", err)
	}
}

// Simulates a crash in the middle of an earlier save: a half-written
// temporary file was left behind. The real file must have been untouched
// by it, and the next save must work normally.
func TestFileStorage_LeftoverPartialTempFileIsHarmless(t *testing.T) {
	s, _ := NewFileStorage(t.TempDir())
	s.Save(PersistentState{CurrentTerm: 4, VotedFor: 3})
	os.WriteFile(s.Path()+".tmp", []byte(`{"version":1,"currentTer`), 0o644) // torn write

	if f := readStateFile(t, s); f.CurrentTerm != 4 || f.VotedFor != 3 {
		t.Fatalf("a torn temp file affected the real state: %+v", f)
	}
	if err := s.Save(PersistentState{CurrentTerm: 5, VotedFor: -1}); err != nil {
		t.Fatalf("Save after a torn temp file: %v", err)
	}
	if f := readStateFile(t, s); f.CurrentTerm != 5 {
		t.Fatalf("got %+v after saving again", f)
	}
}

func TestFileStorage_SaveFailsCleanlyWhenDirectoryIsGone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node1")
	s, _ := NewFileStorage(dir)
	os.RemoveAll(dir)
	if err := s.Save(PersistentState{CurrentTerm: 1}); err == nil {
		t.Fatal("expected an error when the data directory no longer exists")
	}
}