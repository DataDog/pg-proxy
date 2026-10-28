//go:build integration

package itest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// readyAddress extracts the listener address from the binary's startup log line.
var readyAddress = regexp.MustCompile(`\baddress=("(?:[^"\\]|\\.)*"|\S+)`)

// Proxy is a running pg-proxy process.
type Proxy struct {
	// Addr is the host:port the proxy listens on.
	Addr string
	// ConfigPath is the YAML file the proxy was started with.
	ConfigPath string

	dir      string
	cmd      *exec.Cmd
	logs     logBuffer
	done     chan struct{}
	err      error
	stopOnce sync.Once
}

// StartProxy writes cfg to a YAML file, starts the pg-proxy binary with it, and
// waits until the listener is bound. An empty listener address binds a free
// loopback port. Callers must Stop the proxy.
func StartProxy(ctx context.Context, cfg pgconfig.Config) (*Proxy, error) {
	binary, err := Binary()
	if err != nil {
		return nil, err
	}
	if cfg.Proxy.ListenerAddress == "" {
		cfg.Proxy.ListenerAddress = "127.0.0.1:0"
	}
	data, err := encodeConfig(cfg)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "pg-proxy-itest-config-")
	if err != nil {
		return nil, err
	}
	p := &Proxy{ConfigPath: filepath.Join(dir, "proxy.yaml"), dir: dir, done: make(chan struct{})}
	if err := os.WriteFile(p.ConfigPath, data, 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	p.cmd = exec.Command(binary, "-config", p.ConfigPath)
	p.cmd.Stdout = &p.logs
	stderr, err := p.cmd.StderrPipe()
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if err := p.cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("start pg-proxy: %w", err)
	}
	ready := make(chan string, 1)
	go p.watch(stderr, ready)
	select {
	case p.Addr = <-ready:
		return p, nil
	case <-p.done:
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("pg-proxy exited before it was ready: %v\n%s", p.err, p.Logs())
	case <-ctx.Done():
		_ = p.Stop()
		return nil, fmt.Errorf("pg-proxy was not ready: %w\n%s", ctx.Err(), p.Logs())
	}
}

// MustStartProxy starts a proxy for the duration of t. Cleanup stops it, fails t
// if it does not exit cleanly, and logs its output if t failed.
func MustStartProxy(t testing.TB, cfg pgconfig.Config) *Proxy {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := StartProxy(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Stop(); err != nil {
			t.Errorf("pg-proxy did not exit cleanly: %v", err)
		}
		if t.Failed() {
			t.Logf("pg-proxy output:\n%s", p.Logs())
		}
	})
	return p
}

// watch copies the process output into the log buffer, reports the listener
// address once, and records the exit status after the output closes.
func (p *Proxy) watch(stderr io.Reader, ready chan<- string) {
	scanner := bufio.NewScanner(stderr)
	reported := false
	for scanner.Scan() {
		line := scanner.Text()
		_, _ = p.logs.Write([]byte(line + "\n"))
		if reported || !strings.Contains(line, "local proxy ready") {
			continue
		}
		if match := readyAddress.FindStringSubmatch(line); match != nil {
			address := match[1]
			if unquoted, err := strconv.Unquote(address); err == nil {
				address = unquoted
			}
			ready <- address
			reported = true
		}
	}
	// Drain the rest so the process never blocks on a full pipe.
	_, _ = io.Copy(&p.logs, stderr)
	p.err = p.cmd.Wait()
	close(p.done)
}

// URL returns a connection URL for a logical database through the proxy.
func (p *Proxy) URL(database, user string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.User(user),
		Host:     p.Addr,
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// Connect opens a client connection to a logical database through the proxy.
func (p *Proxy) Connect(ctx context.Context, database, user string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, p.URL(database, user))
}

// Signal sends sig to the proxy process.
func (p *Proxy) Signal(sig os.Signal) error {
	return p.cmd.Process.Signal(sig)
}

// Done is closed when the proxy process exits; Err then reports its exit status.
func (p *Proxy) Done() <-chan struct{} { return p.done }

// Err returns the exit status once Done is closed.
func (p *Proxy) Err() error {
	select {
	case <-p.done:
		return p.err
	default:
		return errors.New("pg-proxy is still running")
	}
}

// Stop sends SIGTERM, kills the process if it has not exited within 15 seconds,
// removes its configuration, and returns its exit status.
func (p *Proxy) Stop() error {
	p.stopOnce.Do(func() {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(15 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
		_ = os.RemoveAll(p.dir)
	})
	return p.err
}

// Logs returns everything the process has written so far.
func (p *Proxy) Logs() string {
	return p.logs.String()
}

// encodeConfig marshals cfg to YAML. Secret redacts itself when marshaled, so
// database passwords are written back into the encoded document.
func encodeConfig(cfg pgconfig.Config) ([]byte, error) {
	var document yaml.Node
	if err := document.Encode(cfg); err != nil {
		return nil, fmt.Errorf("encode pg-proxy config: %w", err)
	}
	if databases := mappingValue(&document, "databases"); databases != nil {
		for i, entry := range databases.Content {
			if password := mappingValue(entry, "password"); password != nil {
				password.Value = string(cfg.Databases[i].Password)
			}
		}
	}
	return yaml.Marshal(&document)
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
