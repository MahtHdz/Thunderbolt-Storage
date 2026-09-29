package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunHonorsStoreFlagAndRoundTrips(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "custom-store")
	source := filepath.Join(root, "source.txt")
	dest := filepath.Join(root, "out", "dest.txt")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"put", "--store", storeDir, "id-1", source}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("put exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(storeDir, "objects")); err != nil {
		t.Fatalf("custom store was not used: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	code = run(context.Background(), []string{"get", "--store", storeDir, "id-1", dest}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("get exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "payload" {
		t.Fatalf("got %q", got)
	}
}

func TestRunRejectsDuplicateWriteIDs(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	_ = os.WriteFile(a, []byte("a"), 0o600)
	_ = os.WriteFile(b, []byte("b"), 0o600)

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"put", "--store", filepath.Join(root, "store"), "same", a, "same", b}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "duplicate object ID") {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}
}

func TestSubcommandHelpReturnsSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"get", "--help"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("help exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage of get:") {
		t.Fatalf("expected usage in stdout, got %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestGetRejectsNonPositiveWorkersAsUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"get", "--workers", "0", "id", "out"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "workers must be greater than zero") {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}
}

func TestTrailingFlagsAndShortFlags(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	source := filepath.Join(root, "source.txt")
	dest := filepath.Join(root, "dest.txt")
	if err := os.WriteFile(source, []byte("hello flags"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	// Test short flags -s, -w, -j and trailing position
	code := run(context.Background(), []string{"put", "obj1", source, "-s", storeDir, "-w", "2", "-j"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("put exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), `"operation":"put"`) {
		t.Fatalf("expected json output: %s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	// Test trailing flag --checksum on stat
	code = run(context.Background(), []string{"stat", "obj1", "-s", storeDir, "--checksum"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("stat exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "verified=true") {
		t.Fatalf("expected verified=true: %s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	// Test short flag -o on get with trailing position
	_ = os.WriteFile(dest, []byte("old"), 0o600)
	code = run(context.Background(), []string{"get", "obj1", dest, "-s", storeDir, "-o"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("get exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	got, _ := os.ReadFile(dest)
	if string(got) != "hello flags" {
		t.Fatalf("unexpected content: %s", string(got))
	}

	stdout.Reset()
	stderr.Reset()
	// Test short flag -m on delete
	code = run(context.Background(), []string{"delete", "-s", storeDir, "-m", "nonexistent-id"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("delete exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestEnvVarStore(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "env-store")
	source := filepath.Join(root, "source.txt")
	_ = os.WriteFile(source, []byte("env-data"), 0o600)

	t.Setenv("TB_STORE", storeDir)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"put", "env-id", source}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("put exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(storeDir, "objects")); err != nil {
		t.Fatalf("store from TB_STORE not created: %v", err)
	}

	t.Setenv("THUNDERBOLT_STORE", filepath.Join(root, "tb-store"))
	if defaultStore() != filepath.Join(root, "tb-store") {
		t.Fatalf("expected THUNDERBOLT_STORE to take precedence")
	}
}

func TestCompletion(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish"} {
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), []string{"completion", shell}, &stdout, &stderr)
		if code != exitOK || stdout.Len() == 0 {
			t.Fatalf("completion %s failed: code=%d", shell, code)
		}
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"completion"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("expected usage error without shell")
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"completion", "unknown"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("expected usage error with unknown shell")
	}
}

func TestPathDoesNotMutateFilesystem(t *testing.T) {
	nonexistent := filepath.Join(t.TempDir(), "should-not-exist")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"path", "-s", nonexistent, "myid"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("path exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(nonexistent); !os.IsNotExist(err) {
		t.Fatalf("path should not create store directory")
	}
}
