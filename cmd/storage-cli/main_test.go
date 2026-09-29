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
