package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/teddymalhan/pallasdb/db"
)

// fsmtestSink is an in-memory raft.SnapshotSink.
type fsmtestSink struct {
	buf       bytes.Buffer
	cancelled bool
	closed    bool
}

func (s *fsmtestSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *fsmtestSink) Close() error                { s.closed = true; return nil }
func (s *fsmtestSink) Cancel() error               { s.cancelled = true; return nil }
func (s *fsmtestSink) ID() string                  { return "fsmtest" }

// fsmtestNew creates an FSM over a fresh data directory.
func fsmtestNew(t *testing.T) *FSM {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	store, err := db.NewKV(dir)
	if err != nil {
		t.Fatalf("db.NewKV(%s): %v", dir, err)
	}
	fsm := NewFSM(store, dir)
	t.Cleanup(func() { _ = fsm.Close() })
	return fsm
}

func fsmtestApply(t *testing.T, fsm *FSM, index uint64, cmd Command) *FSMResult {
	t.Helper()
	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand(%+v): %v", cmd, err)
	}
	return fsmtestApplyRaw(t, fsm, index, data)
}

func fsmtestApplyRaw(t *testing.T, fsm *FSM, index uint64, data []byte) *FSMResult {
	t.Helper()
	res, ok := fsm.Apply(&raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: data}).(*FSMResult)
	if !ok {
		t.Fatalf("Apply returned %T, want *FSMResult", res)
	}
	return res
}

func fsmtestPut(t *testing.T, fsm *FSM, index uint64, key, val string) {
	t.Helper()
	res := fsmtestApply(t, fsm, index, Command{Op: OpPut, Key: []byte(key), Val: []byte(val), Mode: int(db.ModeUpsert)})
	if res.Err != nil {
		t.Fatalf("put %q: %v", key, res.Err)
	}
}

func fsmtestWant(t *testing.T, fsm *FSM, key, want string) {
	t.Helper()
	val, ok, err := fsm.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !ok {
		t.Fatalf("Get(%q): missing, want %q", key, want)
	}
	if string(val) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, val, want)
	}
}

func fsmtestWantMissing(t *testing.T, fsm *FSM, key string) {
	t.Helper()
	val, ok, err := fsm.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if ok {
		t.Fatalf("Get(%q) = %q, want missing", key, val)
	}
}

// fsmtestWantFatal asserts fn aborts the node instead of diverging silently.
func fsmtestWantFatal(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected the FSM to fail hard, but it returned normally")
		}
		msg, ok := r.(string)
		if !ok || !strings.HasPrefix(msg, "cluster: FSM cannot continue") {
			t.Fatalf("unexpected panic value %v", r)
		}
	}()
	fn()
}

// fsmtestSnapshot persists a snapshot of fsm and returns the bytes.
func fsmtestSnapshot(t *testing.T, fsm *FSM) []byte {
	t.Helper()
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Release()
	sink := &fsmtestSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if sink.cancelled || !sink.closed {
		t.Fatalf("sink state: cancelled=%v closed=%v", sink.cancelled, sink.closed)
	}
	return sink.buf.Bytes()
}

func TestApplyPutAndDelete(t *testing.T) {
	fsm := fsmtestNew(t)

	res := fsmtestApply(t, fsm, 1, Command{Op: OpPut, Key: []byte("k"), Val: []byte("v"), Mode: int(db.ModeUpsert)})
	if res.Err != nil || !res.Updated {
		t.Fatalf("put: %+v", res)
	}
	fsmtestWant(t, fsm, "k", "v")

	// Insert on an existing key is a deterministic conflict: no error, no update.
	res = fsmtestApply(t, fsm, 2, Command{Op: OpPut, Key: []byte("k"), Val: []byte("other"), Mode: int(db.ModeInsert)})
	if res.Err != nil {
		t.Fatalf("insert conflict returned an error: %v", res.Err)
	}
	if res.Updated {
		t.Fatal("insert on an existing key reported an update")
	}
	fsmtestWant(t, fsm, "k", "v")

	// Update on a missing key is likewise a no-op.
	res = fsmtestApply(t, fsm, 3, Command{Op: OpPut, Key: []byte("absent"), Val: []byte("v"), Mode: int(db.ModeUpdate)})
	if res.Err != nil || res.Updated {
		t.Fatalf("update on missing key: %+v", res)
	}

	res = fsmtestApply(t, fsm, 4, Command{Op: OpDel, Key: []byte("k")})
	if res.Err != nil || !res.Updated {
		t.Fatalf("delete: %+v", res)
	}
	fsmtestWantMissing(t, fsm, "k")

	res = fsmtestApply(t, fsm, 5, Command{Op: OpDel, Key: []byte("k")})
	if res.Err != nil || res.Updated {
		t.Fatalf("delete of a missing key: %+v", res)
	}
}

// An out-of-range mode is deterministic: every replica rejects it identically,
// so it comes back as a result instead of taking the node down.
func TestApplyInvalidModeIsDeterministicRejection(t *testing.T) {
	fsm := fsmtestNew(t)

	for _, mode := range []int{0, 4, 99} {
		data, err := EncodeCommand(Command{Op: OpPut, Key: []byte("k"), Val: []byte("v"), Mode: mode})
		if err != nil {
			t.Fatalf("EncodeCommand(mode=%d): %v", mode, err)
		}
		res := fsmtestApplyRaw(t, fsm, uint64(mode+1), data)
		if res.Err == nil {
			t.Fatalf("mode %d: expected a rejection", mode)
		}
		fsmtestWantMissing(t, fsm, "k")
	}

	// The FSM stays usable and keeps tracking indexes.
	fsmtestPut(t, fsm, 10, "k", "v")
	fsmtestWant(t, fsm, "k", "v")
}

func TestApplyUnknownOpFailsHard(t *testing.T) {
	fsm := fsmtestNew(t)
	fsmtestWantFatal(t, func() { fsm.applyCommand(7, Command{Op: "op-from-the-future"}) })
}

func TestApplyUndecodableEntryFailsHard(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"garbage", []byte{0x42, 0x00, 0x01}},
		{"newer format version", []byte{commandMagic, CommandVersion + 1, opCodePut, 0x01, 'k', 0x00, 0x01}},
		{"legacy json with unknown op", []byte(`{"op":"merge","key":"aGk="}`)},
		{"truncated", []byte{commandMagic, CommandVersion, opCodePut, 0x05, 'k'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := fsmtestNew(t)
			fsmtestWantFatal(t, func() {
				fsm.Apply(&raft.Log{Index: 3, Type: raft.LogCommand, Data: tc.data})
			})
		})
	}
}

// A log entry written by the previous release is JSON; it must still replay.
func TestApplyLegacyJSONEntry(t *testing.T) {
	fsm := fsmtestNew(t)

	res := fsmtestApplyRaw(t, fsm, 1, []byte(`{"op":"put","key":"aGVsbG8=","val":"d29ybGQ=","mode":1}`))
	if res.Err != nil || !res.Updated {
		t.Fatalf("legacy put: %+v", res)
	}
	fsmtestWant(t, fsm, "hello", "world")

	res = fsmtestApplyRaw(t, fsm, 2, []byte(`{"op":"del","key":"aGVsbG8="}`))
	if res.Err != nil || !res.Updated {
		t.Fatalf("legacy del: %+v", res)
	}
	fsmtestWantMissing(t, fsm, "hello")
}

func TestApplyBatchIsAtomic(t *testing.T) {
	fsm := fsmtestNew(t)
	fsmtestPut(t, fsm, 1, "keep", "old")
	fsmtestPut(t, fsm, 2, "drop", "gone-soon")

	res := fsmtestApply(t, fsm, 3, NewBatchCommand(
		Mutation{Op: OpPut, Key: []byte("keep"), Val: []byte("new"), Mode: int(db.ModeUpsert)},
		Mutation{Op: OpPut, Key: []byte("fresh"), Val: []byte("v"), Mode: int(db.ModeInsert)},
		Mutation{Op: OpDel, Key: []byte("drop")},
	))
	if res.Err != nil || !res.Updated {
		t.Fatalf("batch: %+v", res)
	}
	fsmtestWant(t, fsm, "keep", "new")
	fsmtestWant(t, fsm, "fresh", "v")
	fsmtestWantMissing(t, fsm, "drop")
}

func TestApplyBatchRejectionAppliesNothing(t *testing.T) {
	fsm := fsmtestNew(t)
	fsmtestPut(t, fsm, 1, "existing", "v0")

	data, err := EncodeCommand(Command{Op: OpBatch, Batch: []Mutation{
		{Op: OpPut, Key: []byte("first"), Val: []byte("v1"), Mode: int(db.ModeUpsert)},
		{Op: OpPut, Key: []byte("second"), Val: []byte("v2"), Mode: 42}, // invalid mode
		{Op: OpDel, Key: []byte("existing")},
	}})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}

	res := fsmtestApplyRaw(t, fsm, 2, data)
	if res.Err == nil {
		t.Fatal("expected the batch to be rejected")
	}
	fsmtestWantMissing(t, fsm, "first")
	fsmtestWantMissing(t, fsm, "second")
	fsmtestWant(t, fsm, "existing", "v0")

	// The transaction was aborted cleanly, so the FSM keeps working.
	fsmtestPut(t, fsm, 3, "after", "ok")
	fsmtestWant(t, fsm, "after", "ok")
}

func TestApplyBatchUnknownMutationFailsHard(t *testing.T) {
	fsm := fsmtestNew(t)
	fsmtestWantFatal(t, func() {
		fsm.applyCommand(4, Command{Op: OpBatch, Batch: []Mutation{
			{Op: OpPut, Key: []byte("k"), Val: []byte("v"), Mode: int(db.ModeUpsert)},
			{Op: "merge", Key: []byte("k2")},
		}})
	})
}

func TestAppliedIndexTracking(t *testing.T) {
	fsm := fsmtestNew(t)
	if got := fsm.AppliedIndex(); got != 0 {
		t.Fatalf("initial AppliedIndex = %d, want 0", got)
	}

	fsmtestPut(t, fsm, 7, "a", "1")
	if got := fsm.AppliedIndex(); got != 7 {
		t.Fatalf("AppliedIndex = %d, want 7", got)
	}

	fsmtestPut(t, fsm, 9, "b", "2")
	if got := fsm.AppliedIndex(); got != 9 {
		t.Fatalf("AppliedIndex = %d, want 9", got)
	}

	// Indexes never move backwards.
	fsm.ObserveAppliedIndex(3)
	if got := fsm.AppliedIndex(); got != 9 {
		t.Fatalf("AppliedIndex went backwards to %d", got)
	}
	fsm.ObserveAppliedIndex(12)
	if got := fsm.AppliedIndex(); got != 12 {
		t.Fatalf("AppliedIndex = %d, want 12", got)
	}

	// Configuration entries never reach Apply, but they do advance the index.
	fsm.StoreConfiguration(15, raft.Configuration{})
	if got := fsm.AppliedIndex(); got != 15 {
		t.Fatalf("AppliedIndex = %d, want 15", got)
	}
}

func TestWaitForAppliedIndex(t *testing.T) {
	fsm := fsmtestNew(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// An already-satisfied target returns immediately.
	if err := fsm.WaitForAppliedIndex(ctx, 0); err != nil {
		t.Fatalf("WaitForAppliedIndex(0): %v", err)
	}

	const target = 5
	errs := make(chan error, 4)
	for range cap(errs) {
		go func() { errs <- fsm.WaitForAppliedIndex(ctx, target) }()
	}

	for i := uint64(1); i <= target; i++ {
		fsmtestPut(t, fsm, i, fmt.Sprintf("k%d", i), "v")
	}
	for range cap(errs) {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("WaitForAppliedIndex: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("WaitForAppliedIndex did not return after the index was reached")
		}
	}
}

func TestWaitForAppliedIndexContextCancelled(t *testing.T) {
	fsm := fsmtestNew(t)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fsm.WaitForAppliedIndex(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := fsm.WaitForAppliedIndex(ctx, 1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("wait took %v", elapsed)
	}

	// A cancelled wait leaves the FSM usable for the next one.
	if err := fsm.WaitForAppliedIndex(context.Background(), 0); err != nil {
		t.Fatalf("WaitForAppliedIndex(0): %v", err)
	}
}

// Raft never routes no-op or barrier entries to an FSM, so a waiter blocked on
// such an index is released by the leader's fence (Node.fenceAppliedIndex)
// calling ObserveAppliedIndex rather than by anything reaching Apply.
func TestWaitForAppliedIndexReleasedByObserve(t *testing.T) {
	fsm := fsmtestNew(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- fsm.WaitForAppliedIndex(ctx, 42) }()

	// The waiter must still be blocked: nothing has applied index 42.
	select {
	case err := <-done:
		t.Fatalf("WaitForAppliedIndex returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	fsm.ObserveAppliedIndex(42) // what the leader's barrier fence does

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForAppliedIndex: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never observed the fenced index")
	}
	if got := fsm.AppliedIndex(); got != 42 {
		t.Fatalf("AppliedIndex = %d, want 42", got)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	source := fsmtestNew(t)
	const n = 500
	for i := range n {
		fsmtestPut(t, source, uint64(i+1), fmt.Sprintf("key:%05d", i), strings.Repeat("v", 1+i%97))
	}
	data := fsmtestSnapshot(t, source)

	target := fsmtestNew(t)
	fsmtestPut(t, target, 1, "stale", "must-not-survive")

	if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for i := range n {
		fsmtestWant(t, target, fmt.Sprintf("key:%05d", i), strings.Repeat("v", 1+i%97))
	}
	fsmtestWantMissing(t, target, "stale")

	// The restored store is live: writes and reads both work.
	fsmtestPut(t, target, 2, "after-restore", "ok")
	fsmtestWant(t, target, "after-restore", "ok")

	// No scaffolding directories are left behind.
	for _, suffix := range []string{restoreDirSuffix, oldDirSuffix} {
		if _, err := os.Stat(target.dirpath + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s still exists (err=%v)", target.dirpath+suffix, err)
		}
	}
}

// A cluster node that never compacts grows its WAL without bound.
func TestRestoredStoreAutoCompacts(t *testing.T) {
	source := fsmtestNew(t)
	fsmtestPut(t, source, 1, "k", "v")
	data := fsmtestSnapshot(t, source)

	target := fsmtestNew(t)
	if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !target.store.Options.AutoCompact {
		t.Fatal("restored store has auto-compaction disabled")
	}
}

func TestRestoreEmptySnapshot(t *testing.T) {
	source := fsmtestNew(t)
	data := fsmtestSnapshot(t, source)

	target := fsmtestNew(t)
	fsmtestPut(t, target, 1, "stale", "v")
	if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	fsmtestWantMissing(t, target, "stale")
	fsmtestPut(t, target, 2, "fresh", "v")
	fsmtestWant(t, target, "fresh", "v")
}

// Regression: a corrupt stream must never reach the live data directory.
func TestRestoreCorruptSnapshotKeepsLiveData(t *testing.T) {
	source := fsmtestNew(t)
	fsmtestPut(t, source, 1, "a", "1")
	fsmtestPut(t, source, 2, "b", "2")
	good := fsmtestSnapshot(t, source)

	cases := map[string][]byte{
		"bad magic":   append([]byte("XXXX"), good[4:]...),
		"flipped crc": append(append([]byte(nil), good[:len(good)-1]...), good[len(good)-1]^0xff),
		"truncated":   good[:len(good)/2],
		"garbage":     []byte("not a snapshot at all"),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			target := fsmtestNew(t)
			fsmtestPut(t, target, 1, "live", "data")

			if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err == nil {
				t.Fatal("expected Restore to fail")
			}
			fsmtestWant(t, target, "live", "data")
			fsmtestPut(t, target, 2, "still", "writable")
			fsmtestWant(t, target, "still", "writable")

			if _, err := os.Stat(target.dirpath + restoreDirSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("restore dir survived a failed restore (err=%v)", err)
			}
		})
	}
}

// Regression for the destroy-then-rename swap: if installing the restored
// directory fails, the original data must survive and the store must still be
// open and usable.
func TestRestoreRenameFailureRollsBack(t *testing.T) {
	source := fsmtestNew(t)
	fsmtestPut(t, source, 1, "restored", "yes")
	data := fsmtestSnapshot(t, source)

	for _, tc := range []struct {
		name       string
		failOnPath func(target *FSM, oldpath string) bool
	}{
		{
			name: "install of the restored dir fails",
			failOnPath: func(target *FSM, oldpath string) bool {
				return oldpath == target.dirpath+restoreDirSuffix
			},
		},
		{
			name: "stashing the live dir fails",
			failOnPath: func(target *FSM, oldpath string) bool {
				return oldpath == target.dirpath
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := fsmtestNew(t)
			fsmtestPut(t, target, 1, "live", "data")
			fsmtestPut(t, target, 2, "live2", "data2")

			injected := errors.New("injected rename failure")
			calls := 0
			target.renameFn = func(oldpath, newpath string) error {
				calls++
				if tc.failOnPath(target, oldpath) {
					return injected
				}
				return os.Rename(oldpath, newpath)
			}

			err := target.Restore(io.NopCloser(bytes.NewReader(data)))
			if err == nil {
				t.Fatal("expected Restore to fail")
			}
			if !errors.Is(err, injected) {
				t.Fatalf("err = %v, want it to wrap the injected failure", err)
			}
			if calls == 0 {
				t.Fatal("the rename seam was never exercised")
			}
			target.renameFn = os.Rename

			// The original data survived...
			fsmtestWant(t, target, "live", "data")
			fsmtestWant(t, target, "live2", "data2")
			fsmtestWantMissing(t, target, "restored")

			// ...the store is open, not a closed handle...
			fsmtestPut(t, target, 3, "written", "after-failure")
			fsmtestWant(t, target, "written", "after-failure")

			// ...and nothing was left lying around.
			for _, suffix := range []string{restoreDirSuffix, oldDirSuffix} {
				if _, err := os.Stat(target.dirpath + suffix); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("%s survived (err=%v)", target.dirpath+suffix, err)
				}
			}

			// A retry without the injected failure succeeds.
			if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
				t.Fatalf("retry Restore: %v", err)
			}
			fsmtestWant(t, target, "restored", "yes")
			fsmtestWantMissing(t, target, "live")
		})
	}
}

func TestRestoreIntoMissingDataDir(t *testing.T) {
	source := fsmtestNew(t)
	fsmtestPut(t, source, 1, "k", "v")
	data := fsmtestSnapshot(t, source)

	target := fsmtestNew(t)
	if err := target.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := os.RemoveAll(target.dirpath); err != nil {
		t.Fatalf("remove data dir: %v", err)
	}
	store, err := db.NewKV(target.dirpath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	target.store = store

	// The point is to leave the FSM with a store whose live data directory has
	// been removed out from under it. On Windows a directory that still holds an
	// open store cannot be deleted (the WAL handle pins it), so the reopened
	// store must be closed first. Restore has to cope with a missing data dir
	// either way.
	if runtime.GOOS == "windows" {
		if err := target.store.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	}
	if err := os.RemoveAll(target.dirpath); err != nil {
		t.Fatalf("remove data dir: %v", err)
	}

	if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore with no live data dir: %v", err)
	}
	fsmtestWant(t, target, "k", "v")
}

func TestCloseIsIdempotentAndGuardsReads(t *testing.T) {
	fsm := fsmtestNew(t)
	fsmtestPut(t, fsm, 1, "k", "v")

	if err := fsm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := fsm.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, _, err := fsm.Get([]byte("k")); !errors.Is(err, ErrFSMClosed) {
		t.Fatalf("Get after Close: err = %v, want %v", err, ErrFSMClosed)
	}
	if _, err := fsm.Snapshot(); !errors.Is(err, ErrFSMClosed) {
		t.Fatalf("Snapshot after Close: err = %v, want %v", err, ErrFSMClosed)
	}
	fsmtestWantFatal(t, func() { fsmtestPut(t, fsm, 2, "k", "v2") })
}

func TestFSMConcurrentReadsDuringApply(t *testing.T) {
	fsm := fsmtestNew(t)
	fsmtestPut(t, fsm, 1, "k", "v")

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, _, err := fsm.Get([]byte("k")); err != nil {
				t.Errorf("Get: %v", err)
				return
			}
			_ = fsm.AppliedIndex()
		}
	}()

	for i := uint64(2); i < 200; i++ {
		fsmtestPut(t, fsm, i, "k", fmt.Sprintf("v%d", i))
	}
	close(stop)
	<-done
}
