//go:build unix

package cluster

import (
	"os"
	"path/filepath"
)

// syncParentDir fsyncs the directory containing path so a rename into it is
// durable.
func syncParentDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
