package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MahtHdz/Thunderbolt-Storage/internal/storage"
)

func invoke(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, err bytes.Buffer
	code := run(context.Background(), args, &out, &err)
	return code, out.String(), err.String()
}
func TestCommandLifecycleJSON(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	os.WriteFile(src, []byte("payload"), 0600)
	for _, command := range []string{"create", "put", "upload", "update", "stat", "path"} {
		args := []string{command, "--json", "--store", store}
		if command == "stat" {
			args = append(args, "--checksum")
		}
		args = append(args, "id")
		if command != "stat" && command != "path" {
			args = append(args, src)
		}
		code, out, err := invoke(t, args...)
		if code != exitOK {
			t.Fatalf("%s: %d %s %s", command, code, out, err)
		}
		var result commandResult
		if e := json.Unmarshal([]byte(out), &result); e != nil {
			t.Fatal(e)
		}
		if result.ID != "id" || result.Error != "" {
			t.Fatal(result)
		}
		if command != "path" && (result.Info == nil || !result.Info.Verified) {
			t.Fatal(result)
		}
	}
	for _, command := range []string{"get", "download"} {
		code, out, err := invoke(t, command, "--json", "--overwrite", "--store", store, "id", dst)
		if code != 0 {
			t.Fatalf("%d %s %s", code, out, err)
		}
	}
	for _, command := range [][]string{
		{"stat", "--store", store, "id"}, {"stat", "--checksum", "--store", store, "id"},
		{"path", "--store", store, "id"}, {"cleanup", "--store", store},
		{"cleanup", "--json", "--store", store}, {"cleanup", "--json", "--downloads", root, "--store", store},
		{"delete", "--store", store, "id"}, {"delete", "--json", "--missing-ok", "--store", store, "id"},
	} {
		if code, out, err := invoke(t, command...); code != 0 {
			t.Fatalf("%v: %d %s %s", command, code, out, err)
		}
	}
}
func TestUsageMatrix(t *testing.T) {
	cases := [][]string{nil, {"unknown"}, {"put"}, {"put", "a"}, {"put", "a", " "}, {"put", "", "p"}, {"put", "--workers", "0", "a", "p"}, {"put", "--workers", "257", "a", "p"}, {"put", "--max-bytes", "-1", "a", "p"}, {"put", "--timeout", "-1s", "a", "p"}, {"get"}, {"get", "--workers", "0", "a", "p"}, {"get", "a", "same", "b", "same"}, {"delete"}, {"delete", "a", "a"}, {"delete", ""}, {"delete", "--workers", "0", "a"}, {"stat"}, {"stat", "--workers", "0", "a"}, {"path"}, {"cleanup", "unexpected"}, {"cleanup", "--older-than", "0"}}
	for _, args := range cases {
		code, _, err := invoke(t, args...)
		if code != exitUsage {
			t.Fatalf("%v: code=%d %s", args, code, err)
		}
	}
	for _, command := range []string{"put", "create", "update", "migrate", "get", "delete", "stat", "path", "cleanup"} {
		if code, _, _ := invoke(t, command, "--help"); code != exitOK {
			t.Fatalf("help %s: %d", command, code)
		}
		if code, _, _ := invoke(t, command, "--invalid-flag"); code != exitUsage {
			t.Fatalf("flag %s: %d", command, code)
		}
	}
	for _, command := range []string{"help", "-h", "--help", "version", "--version", "-version"} {
		if code, out, _ := invoke(t, command); code != exitOK || out == "" {
			t.Fatalf("%s %d", command, code)
		}
	}
}
func TestCommandFailuresAndBatchResults(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	src := filepath.Join(root, "src")
	os.WriteFile(src, []byte("data"), 0600)
	cases := [][]string{
		{"put", "--json", "--store", store, "a", src, "b", filepath.Join(root, "missing")},
		{"create", "--json", "--store", store, "a", src},
		{"update", "--json", "--store", store, "missing", src},
		{"get", "--json", "--store", store, "missing", filepath.Join(root, "out")},
		{"delete", "--json", "--store", store, "missing"},
		{"stat", "--json", "--store", store, "missing"},
		{"path", "--json", "--store", store, ""},
		{"cleanup", "--json", "--store", store, "--downloads", filepath.Join(root, "absent")},
	}
	for _, args := range cases {
		code, out, stderr := invoke(t, args...)
		if code != exitFailure || !strings.Contains(out, "\"error\"") || stderr != "" {
			t.Fatalf("%v: %d %s %s", args, code, out, stderr)
		}
	}
	for _, command := range []string{"put", "get", "delete", "stat", "path", "cleanup"} {
		args := []string{command, "--json", "--store", src}
		if command != "cleanup" {
			args = append(args, "id")
		}
		if command == "put" || command == "get" {
			args = append(args, src)
		}
		if code, out, _ := invoke(t, args...); code != exitFailure || !strings.Contains(out, "error") {
			t.Fatalf("%v: %d %s", args, code, out)
		}
	}
	if code, _, stderr := invoke(t, "delete", "--store", store, "missing"); code != exitFailure || !strings.Contains(stderr, "ERROR") {
		t.Fatal(code, stderr)
	}
}
func TestAliasDestinationsRejected(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	os.Mkdir(real, 0700)
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Skip(err)
	}
	for _, pairPaths := range [][2]string{{filepath.Join(real, "new", "x"), filepath.Join(alias, "new", "x")}, {filepath.Join(real, "X"), filepath.Join(real, "x")}} {
		code, _, err := invoke(t, "get", "--store", filepath.Join(root, "store"), "a", pairPaths[0], "b", pairPaths[1])
		if code != exitUsage || !strings.Contains(err, "duplicate destination") {
			t.Fatal(code, err)
		}
	}
	if err := rejectDuplicateDestinations([]pair{{ID: "a", Path: "bad\x00/child"}}); err == nil {
		t.Fatal("invalid path")
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }
func TestOutputFailures(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	src := filepath.Join(root, "src")
	os.WriteFile(src, []byte("payload"), 0600)
	for _, writer := range []io.Writer{brokenWriter{}, shortWriter{}} {
		for _, args := range [][]string{{"version"}, {"help"}, {"put", "--json", "--store", store, "id", src}, {"cleanup", "--json", "--store", store}, {"cleanup", "--store", store}, {"path", "--store", store, "id"}} {
			var stderr bytes.Buffer
			if code := run(context.Background(), args, writer, &stderr); code != exitFailure || !strings.Contains(stderr.String(), "write stdout") {
				t.Fatalf("%v: %d %s", args, code, stderr.String())
			}
		}
	}
	if code := run(context.Background(), []string{"put", "--help"}, brokenWriter{}, io.Discard); code != exitFailure {
		t.Fatal(code)
	}
	if code := run(context.Background(), []string{"put", "--invalid-flag"}, io.Discard, brokenWriter{}); code != exitFailure {
		t.Fatal(code)
	}
	s, err := storage.New(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(context.Background(), "id", true); err != nil {
		t.Fatal("output failure rolled back mutation", err)
	}
}
func TestCLIContextAndLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code := run(ctx, []string{"put", "id", "missing"}, io.Discard, io.Discard); code != exitInterrupt {
		t.Fatal(code)
	}
	root := t.TempDir()
	src := filepath.Join(root, "src")
	os.WriteFile(src, []byte("long"), 0600)
	if code, _, _ := invoke(t, "put", "--store", filepath.Join(root, "store"), "--max-bytes", "2", "id", src); code != exitFailure {
		t.Fatal(code)
	}
	for _, command := range []string{"put", "get", "delete", "stat"} {
		args := []string{command, "--timeout", "1ns", "--store", filepath.Join(root, "store"), "id"}
		if command == "put" || command == "get" {
			args = append(args, src)
		}
		if code, out, err := invoke(t, args...); code != exitInterrupt {
			t.Fatalf("%v %d %s %s", args, code, out, err)
		}
	}
	if batchExit(errors.New("error"), false) != exitFailure || batchExit(nil, true) != exitFailure || batchExit(context.Canceled, false) != exitInterrupt {
		t.Fatal("batch exit")
	}
	if len(terminationSignals()) == 0 || defaultWorkers() < 2 || defaultWorkers() > 8 {
		t.Fatal("platform defaults")
	}
}
func TestCLIMigrationAndCommitError(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	s, err := storage.New(store)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := s.ObjectPath("legacy")
	os.MkdirAll(filepath.Dir(p), 0700)
	os.WriteFile(p, []byte("legacy"), 0600)
	sum := sha256.Sum256([]byte("legacy"))
	if code, out, err := invoke(t, "migrate", "--json", "--store", store, "legacy", hex.EncodeToString(sum[:])); code != exitOK || !strings.Contains(out, "verified") {
		t.Fatal(code, out, err)
	}
	var out bytes.Buffer
	printSingleError(true, "put", "id", "path", &storage.CommitError{Path: "path", Err: errors.New("sync")}, &out, io.Discard)
	var result commandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.StateChanged {
		t.Fatal(result, err)
	}
	info := storage.ObjectInfo{ID: "id", ModTime: time.Now()}
	printResult(false, commandResult{Operation: "stat", ID: "id", Info: &info}, io.Discard, io.Discard)
}
func FuzzParsePairs(f *testing.F) {
	f.Add("id", "path")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, id, path string) {
		pairs, err := parsePairs([]string{id, path})
		if err == nil {
			if len(pairs) != 1 || pairs[0].ID != id || pairs[0].Path != path || storage.ValidateID(id) != nil || strings.TrimSpace(path) == "" {
				t.Fatal(pairs)
			}
		}
	})
}
