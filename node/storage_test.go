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

func TestFileStorage_LoadWithNoFileMeansFreshStart(t *testing.T) {
	s, _ := NewFileStorage(t.TempDir())
	_, found, err := s.Load()
	if err != nil || found {
		t.Fatalf("Load on an empty directory: found=%v err=%v, want found=false and no error", found, err)
	}
}

func TestFileStorage_LoadReturnsWhatWasSaved(t *testing.T) {
	s, _ := NewFileStorage(t.TempDir())
	saved := PersistentState{CurrentTerm: 7, VotedFor: 3, Log: entriesWithTerms(1, 2, 2, 7)}
	if err := s.Save(saved); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := s.Load()
	if err != nil || !found {
		t.Fatalf("Load: found=%v err=%v", found, err)
	}
	if !samePersistent(saved, loaded) {
		t.Fatalf("loaded %+v, saved %+v", loaded, saved)
	}
}

// Damaged files must be reported, never treated as "no saved state":
// starting over from term 0 with an empty log could make the node vote
// twice in a term or lose committed entries.
func TestFileStorage_LoadRejectsCorruptFiles(t *testing.T) {
	cases := map[string]string{
		"truncated JSON":           `{"version":1,"currentTerm":3,"vot`,
		"not JSON at all":          "\x00\x01garbage",
		"unknown format version":   `{"version":99,"currentTerm":1,"votedFor":-1,"log":[]}`,
		"gap in log indexes":       `{"version":1,"currentTerm":2,"votedFor":-1,"log":[{"Term":1,"Index":1},{"Term":1,"Index":3}]}`,
		"term goes backwards":      `{"version":1,"currentTerm":3,"votedFor":-1,"log":[{"Term":3,"Index":1},{"Term":2,"Index":2}]}`,
		"entry from a future term": `{"version":1,"currentTerm":2,"votedFor":-1,"log":[{"Term":5,"Index":1}]}`,
		"invalid votedFor":         `{"version":1,"currentTerm":2,"votedFor":-7,"log":[]}`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			s, _ := NewFileStorage(t.TempDir())
			os.WriteFile(s.Path(), []byte(content), 0o644)
			_, found, err := s.Load()
			if err == nil {
				t.Fatalf("a corrupt file was accepted (found=%v)", found)
			}
		})
	}
}

// A crash mid-save leaves a torn temp file; the real file is the last
// complete state, and that's what Load must return.
func TestFileStorage_LoadIgnoresLeftoverTempFile(t *testing.T) {
	s, _ := NewFileStorage(t.TempDir())
	s.Save(PersistentState{CurrentTerm: 4, VotedFor: 2, Log: entriesWithTerms(1, 4)})
	os.WriteFile(s.Path()+".tmp", []byte(`{"version":1,"currentTer`), 0o644)

	loaded, found, err := s.Load()
	if err != nil || !found || loaded.CurrentTerm != 4 || loaded.VotedFor != 2 || len(loaded.Log) != 2 {
		t.Fatalf("got %+v found=%v err=%v", loaded, found, err)
	}
}

// The reason persistence exists, at the node level: a node that voted for
// candidate 2 in term 5, crashed and restarted must still refuse to vote
// for anyone else in term 5.
func TestRestore_RestartedNodeRemembersItsVote(t *testing.T) {
	s, _ := NewFileStorage(t.TempDir())

	before := NewNode(1, map[int]string{2: "n2", 3: "n3"}, nil)
	before.SetStorage(s)
	if _, granted := before.HandleRequestVote(5, 2, 0, 0); !granted {
		t.Fatal("expected the first vote")
	}

	// "Restart": a brand-new Node, as the process would create, restored
	// from the same directory.
	state, found, err := s.Load()
	if err != nil || !found {
		t.Fatalf("Load: found=%v err=%v", found, err)
	}
	after := NewNode(1, map[int]string{2: "n2", 3: "n3"}, nil)
	if err := after.RestorePersistentState(state); err != nil {
		t.Fatal(err)
	}
	after.SetStorage(s)

	if _, granted := after.HandleRequestVote(5, 3, 0, 0); granted {
		t.Fatal("after a restart the node voted a second time in the same term")
	}
	if _, granted := after.HandleRequestVote(5, 2, 0, 0); !granted {
		t.Fatal("re-granting the same candidate (a retried request) should still work")
	}
	if got := after.CurrentTerm(); got != 5 {
		t.Fatalf("term after restart = %d, want 5", got)
	}
}

func TestRestore_RestartedNodeKeepsItsLogAndStartsAsFollower(t *testing.T) {
	n := NewNode(1, map[int]string{2: "n2"}, nil)
	saved := PersistentState{CurrentTerm: 3, VotedFor: 1, Log: entriesWithTerms(1, 2, 3)}
	if err := n.RestorePersistentState(saved); err != nil {
		t.Fatal(err)
	}
	st := n.Status()
	if st.State != Follower || st.LeaderID != -1 || st.CommitIndex != 0 {
		t.Fatalf("restored node should be a Follower with no known leader and commit 0, got %+v", st)
	}
	if !samePersistent(PersistentState{CurrentTerm: st.Term, VotedFor: 1, Log: st.Log}, saved) {
		t.Fatalf("restored log/term %+v, want %+v", st, saved)
	}

	// A restored log must be the node's own copy, not shared with the caller.
	saved.Log[0].Term = 99
	if n.Status().Log[0].Term == 99 {
		t.Fatal("RestorePersistentState kept a reference to the caller's log")
	}
}

func TestRestore_RejectsInvalidState(t *testing.T) {
	n := NewNode(1, map[int]string{}, nil)
	bad := PersistentState{CurrentTerm: 1, VotedFor: -1, Log: []LogEntry{{Term: 1, Index: 2}}}
	if err := n.RestorePersistentState(bad); err == nil {
		t.Fatal("accepted a log whose first entry has index 2")
	}
	if n.CurrentTerm() != 0 || n.LogLength() != 0 {
		t.Fatal("a rejected restore must leave the node unchanged")
	}
}
