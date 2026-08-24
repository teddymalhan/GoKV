//go:build !unix

package cluster

import (
	"os"
	"path/filepath"
)

// syncParentDir is a best-effort no-op on platforms without a directory fsync
// (Windows, plan9, wasm). A directory cannot be opened and flushed there, so
// there is nothing to sync: the file-level durability guarantee already
// happened when the store reopened each of its files (FlushFileBuffers on
// Windows). It still attempts the open so a future platform that grows the
// capability starts working without a code change, but the failure is not an
// error the caller must act on.
func syncParentDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil
	}
	defer func() { _ = d.Close() }()
	_ = d.Sync()
	return nil
}
