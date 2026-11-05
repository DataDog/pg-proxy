//go:build integration

package itest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

var built struct {
	once sync.Once
	dir  string
	path string
	err  error
}

// Binary returns the pg-proxy binary used by StartProxy, building it on first use.
func Binary() (string, error) {
	if prebuilt := os.Getenv("PG_PROXY_ITEST_BINARY"); prebuilt != "" {
		return prebuilt, nil
	}
	built.once.Do(func() {
		built.dir, built.err = os.MkdirTemp("", "pg-proxy-itest-")
		if built.err != nil {
			return
		}
		built.path = filepath.Join(built.dir, "pg-proxy")
		args := []string{"build"}
		if raceEnabled {
			args = append(args, "-race")
		}
		args = append(args, "-o", built.path, "github.com/DataDog/pg-proxy/cmd/pg-proxy")
		output, err := exec.Command("go", args...).CombinedOutput()
		if err != nil {
			built.err = fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, output)
		}
	})
	return built.path, built.err
}

// removeBinary deletes the binary built by Binary, if any.
func removeBinary() {
	if built.dir != "" {
		_ = os.RemoveAll(built.dir)
	}
}
