package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/MahtHdz/Thunderbolt-Storage/internal/batch"
	"github.com/MahtHdz/Thunderbolt-Storage/internal/storage"
)

const (
	exitOK        = 0
	exitFailure   = 1
	exitUsage     = 2
	exitInterrupt = 130
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

type pair struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

type commandResult struct {
	Operation    string              `json:"operation"`
	ID           string              `json:"id,omitempty"`
	Path         string              `json:"path,omitempty"`
	Info         *storage.ObjectInfo `json:"info,omitempty"`
	StateChanged bool                `json:"stateChanged,omitempty"`
	Error        string              `json:"error,omitempty"`
}

type commonFlags struct {
	store             string
	workers           int
	json              bool
	maxBytes          int64
	timeout           time.Duration
	requireDurability bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), terminationSignals()...)
	go func() { <-ctx.Done(); stop() }()
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// Track every output failure, including help, flags, and JSON encoding.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	out, diagnostic := &checkedWriter{writer: stdout}, &checkedWriter{writer: stderr}
	code := runCommand(ctx, args, out, diagnostic)
	if out.err != nil {
		fmt.Fprintf(stderr, "write stdout: %v\n", out.err)
		if code != exitInterrupt {
			return exitFailure
		}
	}
	if diagnostic.err != nil && code != exitInterrupt {
		return exitFailure
	}
	return code
}

type checkedWriter struct {
	writer io.Writer
	err    error
}

func (w *checkedWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if ctx.Err() != nil {
		return exitInterrupt
	}
	if len(args) == 0 {
		printUsage(stderr)
		return exitUsage
	}

	var code int
	switch args[0] {
	case "put", "upload":
		code = runWrite(ctx, "put", storage.Upsert, args[1:], stdout, stderr)
	case "create":
		code = runWrite(ctx, "create", storage.CreateOnly, args[1:], stdout, stderr)
	case "migrate":
		code = runWrite(ctx, "migrate", storage.Upsert, args[1:], stdout, stderr)
	case "update":
		code = runWrite(ctx, "update", storage.UpdateOnly, args[1:], stdout, stderr)
	case "get", "download":
		code = runGet(ctx, args[1:], stdout, stderr)
	case "delete":
		code = runDelete(ctx, args[1:], stdout, stderr)
	case "stat":
		code = runStat(ctx, args[1:], stdout, stderr)
	case "path":
		code = runPath(ctx, args[1:], stdout, stderr)
	case "cleanup":
		code = runCleanup(ctx, args[1:], stdout, stderr)
	case "completion":
		code = runCompletion(args[1:], stdout, stderr)
	case "version", "--version", "-version":
		fmt.Fprintf(stdout, "storage-cli %s commit=%s built=%s (%s/%s)\n", version, commit, buildDate, runtime.GOOS, runtime.GOARCH)
		return exitOK
	case "help", "-h", "--help":
		printUsage(stdout)
		return exitOK
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return exitUsage
	}

	if ctx.Err() != nil {
		return exitInterrupt
	}
	return code
}

func batchExit(err error, failed bool) int {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return exitInterrupt
	}
	if err != nil || failed {
		return exitFailure
	}
	return exitOK
}

func runWrite(ctx context.Context, operation string, mode storage.WriteMode, args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet(operation)
	setupUsage(fs, operation)
	if err := parseFlags(fs, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if common.workers <= 0 || common.workers > batch.MaxWorkers || common.maxBytes < 0 || common.timeout < 0 {
		fmt.Fprintln(stderr, "workers must be greater than zero and at most 256; max-bytes and timeout must be nonnegative")
		return exitUsage
	}

	pairs, err := parsePairs(fs.Args())
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if err := rejectDuplicateIDs(pairs); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}

	if common.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, common.timeout)
		defer cancel()
	}
	store, err := storage.NewWithOptions(common.store, storage.Options{MaxObjectBytes: common.maxBytes, RequireDurability: common.requireDurability})
	if err != nil {
		printSingleError(common.json, operation, "", "", err, stdout, stderr)
		return exitFailure
	}

	results, batchErr := batch.Run(ctx, common.workers, pairs, func(ctx context.Context, p pair) (storage.ObjectInfo, error) {
		if operation == "migrate" {
			return store.Migrate(ctx, p.ID, p.Path)
		}
		return store.PutFile(ctx, p.ID, p.Path, mode)
	})

	failed := false
	for _, result := range results {
		if result.Err != nil {
			failed = true
			printResult(common.json, commandResult{Operation: operation, ID: result.Value.ID, Path: result.Value.Path, Error: result.Err.Error(), StateChanged: errors.Is(result.Err, storage.ErrCommitUncertain)}, stdout, stderr)
			continue
		}
		info := result.Output
		printResult(common.json, commandResult{Operation: operation, ID: result.Value.ID, Path: result.Value.Path, Info: &info}, stdout, stderr)
	}
	return batchExit(batchErr, failed)
}

func runGet(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("get")
	overwrite := fs.Bool("overwrite", false, "atomically replace an existing destination")
	setupUsage(fs, "get")
	if err := parseFlags(fs, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if common.workers <= 0 || common.workers > batch.MaxWorkers || common.maxBytes < 0 || common.timeout < 0 {
		fmt.Fprintln(stderr, "workers must be greater than zero and at most 256; max-bytes and timeout must be nonnegative")
		return exitUsage
	}
	pairs, err := parsePairs(fs.Args())
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if err := rejectDuplicateDestinations(pairs); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}

	if common.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, common.timeout)
		defer cancel()
	}
	store, err := storage.NewWithOptions(common.store, storage.Options{MaxObjectBytes: common.maxBytes, RequireDurability: common.requireDurability})
	if err != nil {
		printSingleError(common.json, "get", "", "", err, stdout, stderr)
		return exitFailure
	}

	results, batchErr := batch.Run(ctx, common.workers, pairs, func(ctx context.Context, p pair) (storage.ObjectInfo, error) {
		return store.GetFile(ctx, p.ID, p.Path, *overwrite)
	})

	failed := false
	for _, result := range results {
		if result.Err != nil {
			failed = true
			printResult(common.json, commandResult{Operation: "get", ID: result.Value.ID, Path: result.Value.Path, Error: result.Err.Error(), StateChanged: errors.Is(result.Err, storage.ErrCommitUncertain)}, stdout, stderr)
			continue
		}
		info := result.Output
		printResult(common.json, commandResult{Operation: "get", ID: result.Value.ID, Path: result.Value.Path, Info: &info}, stdout, stderr)
	}
	return batchExit(batchErr, failed)
}

func runDelete(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("delete")
	missingOK := fs.Bool("missing-ok", false, "do not fail when an object does not exist")
	setupUsage(fs, "delete")
	if err := parseFlags(fs, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if common.workers <= 0 || common.workers > batch.MaxWorkers || common.maxBytes < 0 || common.timeout < 0 {
		fmt.Fprintln(stderr, "workers must be greater than zero and at most 256; max-bytes and timeout must be nonnegative")
		return exitUsage
	}
	ids := fs.Args()
	if len(ids) == 0 {
		fmt.Fprintln(stderr, "delete requires at least one object ID")
		return exitUsage
	}
	if err := rejectDuplicateStrings(ids, "object ID"); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	for _, id := range ids {
		if err := storage.ValidateID(id); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}

	if common.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, common.timeout)
		defer cancel()
	}
	store, err := storage.NewWithOptions(common.store, storage.Options{MaxObjectBytes: common.maxBytes, RequireDurability: common.requireDurability})
	if err != nil {
		printSingleError(common.json, "delete", "", "", err, stdout, stderr)
		return exitFailure
	}

	results, batchErr := batch.Run(ctx, common.workers, ids, func(ctx context.Context, id string) (struct{}, error) {
		return struct{}{}, store.Delete(ctx, id, *missingOK)
	})
	failed := false
	for _, result := range results {
		if result.Err != nil {
			failed = true
			printResult(common.json, commandResult{Operation: "delete", ID: result.Value, Error: result.Err.Error(), StateChanged: errors.Is(result.Err, storage.ErrCommitUncertain)}, stdout, stderr)
			continue
		}
		printResult(common.json, commandResult{Operation: "delete", ID: result.Value}, stdout, stderr)
	}
	return batchExit(batchErr, failed)
}

func runStat(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("stat")
	checksum := fs.Bool("checksum", false, "compute SHA-256 by reading object contents")
	setupUsage(fs, "stat")
	if err := parseFlags(fs, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if common.workers <= 0 || common.workers > batch.MaxWorkers || common.maxBytes < 0 || common.timeout < 0 {
		fmt.Fprintln(stderr, "workers must be greater than zero and at most 256; max-bytes and timeout must be nonnegative")
		return exitUsage
	}
	ids := fs.Args()
	if len(ids) == 0 {
		fmt.Fprintln(stderr, "stat requires at least one object ID")
		return exitUsage
	}

	if common.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, common.timeout)
		defer cancel()
	}
	store, err := storage.NewWithOptions(common.store, storage.Options{MaxObjectBytes: common.maxBytes, RequireDurability: common.requireDurability})
	if err != nil {
		printSingleError(common.json, "stat", "", "", err, stdout, stderr)
		return exitFailure
	}

	results, batchErr := batch.Run(ctx, common.workers, ids, func(ctx context.Context, id string) (storage.ObjectInfo, error) {
		return store.Stat(ctx, id, *checksum)
	})
	failed := false
	for _, result := range results {
		if result.Err != nil {
			failed = true
			printResult(common.json, commandResult{Operation: "stat", ID: result.Value, Error: result.Err.Error(), StateChanged: errors.Is(result.Err, storage.ErrCommitUncertain)}, stdout, stderr)
			continue
		}
		info := result.Output
		printResult(common.json, commandResult{Operation: "stat", ID: result.Value, Info: &info}, stdout, stderr)
	}
	return batchExit(batchErr, failed)
}

func runPath(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if ctx.Err() != nil {
		return exitInterrupt
	}
	fs := flag.NewFlagSet("path", flag.ContinueOnError)
	setupUsage(fs, "path")
	storePath := fs.String("store", defaultStore(), "storage directory (env: TB_STORE)")
	jsonOutput := fs.Bool("json", false, "emit newline-delimited JSON")
	if err := parseFlags(fs, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	ids := fs.Args()
	if len(ids) == 0 {
		fmt.Fprintln(stderr, "path requires at least one object ID")
		return exitUsage
	}
	failed := false
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return exitInterrupt
		}
		path, err := storage.CalculateObjectPath(*storePath, id)
		if err != nil {
			failed = true
			printResult(*jsonOutput, commandResult{Operation: "path", ID: id, Error: err.Error(), StateChanged: errors.Is(err, storage.ErrCommitUncertain)}, stdout, stderr)
			continue
		}
		printResult(*jsonOutput, commandResult{Operation: "path", ID: id, Path: path}, stdout, stderr)
	}
	if failed {
		return exitFailure
	}
	return exitOK
}

func runCleanup(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if ctx.Err() != nil {
		return exitInterrupt
	}
	fs := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	setupUsage(fs, "cleanup")
	storePath := fs.String("store", defaultStore(), "storage directory (env: TB_STORE)")
	olderThan := fs.Duration("older-than", 24*time.Hour, "only remove orphaned temporary files older than this")
	jsonOutput := fs.Bool("json", false, "emit JSON")
	downloads := fs.String("downloads", "", "clean this download directory instead of object staging files")
	if err := parseFlags(fs, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 0 || *olderThan <= 0 {
		fmt.Fprintln(stderr, "cleanup requires a positive older-than and no positional arguments")
		return exitUsage
	}
	store, err := storage.New(*storePath)
	if err != nil {
		printSingleError(*jsonOutput, "cleanup", "", "", err, stdout, stderr)
		return exitFailure
	}
	var report storage.CleanupReport
	if *downloads != "" {
		report, err = store.CleanupDownloads(ctx, *downloads, *olderThan)
	} else {
		report, err = store.CleanupTemps(ctx, *olderThan)
	}
	if err != nil {
		printSingleError(*jsonOutput, "cleanup", "", "", err, stdout, stderr)
		return exitFailure
	}
	if *jsonOutput {
		_ = json.NewEncoder(stdout).Encode(report)
	} else {
		fmt.Fprintf(stdout, "cleanup: scanned=%d removed=%d skipped=%d\n", report.Scanned, report.Removed, report.Skipped)
	}
	return exitOK
}

func runCompletion(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: storage-cli completion [bash|zsh|fish]")
		return exitUsage
	}
	switch args[0] {
	case "bash":
		fmt.Fprint(stdout, bashCompletionScript)
		return exitOK
	case "zsh":
		fmt.Fprint(stdout, zshCompletionScript)
		return exitOK
	case "fish":
		fmt.Fprint(stdout, fishCompletionScript)
		return exitOK
	default:
		fmt.Fprintf(stderr, "unsupported shell %q; choose bash, zsh, or fish\n", args[0])
		return exitUsage
	}
}

const defaultStoreDir = "./local_store"

func defaultStore() string {
	if s := os.Getenv("THUNDERBOLT_STORE"); strings.TrimSpace(s) != "" {
		return s
	}
	if s := os.Getenv("TB_STORE"); strings.TrimSpace(s) != "" {
		return s
	}
	return defaultStoreDir
}

var shortToLong = map[string]string{
	"s": "store",
	"w": "workers",
	"j": "json",
	"t": "timeout",
	"o": "overwrite",
	"c": "checksum",
	"m": "missing-ok",
}

func reorderArgs(fs *flag.FlagSet, args []string) []string {
	var flags []string
	var positionals []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i:]...)
			break
		}
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
			continue
		}

		name := strings.TrimLeft(arg, "-")
		if name == "h" || name == "help" {
			flags = append(flags, arg)
			continue
		}

		hasEqual := strings.Contains(name, "=")
		var flagName string
		var flagVal string
		if hasEqual {
			parts := strings.SplitN(name, "=", 2)
			flagName = parts[0]
			flagVal = parts[1]
		} else {
			flagName = name
		}

		if long, ok := shortToLong[flagName]; ok && fs.Lookup(long) != nil {
			flagName = long
			if hasEqual {
				arg = "--" + flagName + "=" + flagVal
			} else {
				arg = "--" + flagName
			}
		}

		fl := fs.Lookup(flagName)
		if fl == nil {
			flags = append(flags, arg)
			continue
		}

		isBool := false
		if bf, ok := fl.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			isBool = true
		}

		if hasEqual || isBool {
			flags = append(flags, arg)
		} else {
			flags = append(flags, arg)
			if i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		}
	}

	return append(flags, positionals...)
}

func parseFlags(fs *flag.FlagSet, args []string, stdout, stderr io.Writer) error {
	reordered := reorderArgs(fs, args)
	var errBuf bytes.Buffer
	fs.SetOutput(&errBuf)
	err := fs.Parse(reordered)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(stdout)
			fs.Usage()
			return flag.ErrHelp
		}
		fs.SetOutput(stderr)
		if errBuf.Len() > 0 {
			fmt.Fprint(stderr, errBuf.String())
		}
		return err
	}
	return nil
}

func setupUsage(fs *flag.FlagSet, name string) {
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintf(w, "Usage of %s:\n", name)
		fs.PrintDefaults()
	}
}

func newCommonFlagSet(name string) (*flag.FlagSet, *commonFlags) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	common := commonFlags{}
	fs.StringVar(&common.store, "store", defaultStore(), "storage directory (env: TB_STORE)")
	fs.IntVar(&common.workers, "workers", defaultWorkers(), "maximum concurrent operations")
	fs.BoolVar(&common.requireDurability, "require-durable", false, "reject platforms without directory durability barriers")
	fs.Int64Var(&common.maxBytes, "max-bytes", 0, "maximum object payload bytes (0 unlimited)")
	fs.DurationVar(&common.timeout, "timeout", 0, "operation timeout (0 unlimited)")
	fs.BoolVar(&common.json, "json", false, "emit newline-delimited JSON")
	return fs, &common
}

func defaultWorkers() int {
	n := runtime.NumCPU()
	if n < 2 {
		return 2
	}
	if n > 8 {
		return 8
	}
	return n
}

func parsePairs(args []string) ([]pair, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, fmt.Errorf("expected one or more <id> <path> pairs")
	}
	pairs := make([]pair, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		id, path := args[i], args[i+1]
		if err := storage.ValidateID(id); err != nil {
			return nil, err
		}
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("path for object %q is empty", id)
		}
		pairs = append(pairs, pair{ID: id, Path: path})
	}
	return pairs, nil
}

func rejectDuplicateIDs(pairs []pair) error {
	seen := make(map[string]struct{}, len(pairs))
	for _, p := range pairs {
		if _, ok := seen[p.ID]; ok {
			return fmt.Errorf("duplicate object ID in one command: %q", p.ID)
		}
		seen[p.ID] = struct{}{}
	}
	return nil
}

func rejectDuplicateDestinations(pairs []pair) error {
	seen := make(map[string]string, len(pairs))
	for _, p := range pairs {
		clean, err := storage.DestinationKey(p.Path)
		if err != nil {
			return fmt.Errorf("resolve destination %q: %w", p.Path, err)
		}
		if previous, ok := seen[clean]; ok {
			return fmt.Errorf("duplicate destination %q used by IDs %q and %q", clean, previous, p.ID)
		}
		seen[clean] = p.ID
	}
	return nil
}

func rejectDuplicateStrings(values []string, label string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return fmt.Errorf("duplicate %s: %q", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func printResult(jsonOutput bool, result commandResult, stdout, stderr io.Writer) {
	if jsonOutput {
		_ = json.NewEncoder(stdout).Encode(result)
		return
	}

	if result.Error != "" {
		fmt.Fprintf(stderr, "ERROR operation=%s id=%q path=%q: %s\n", result.Operation, result.ID, result.Path, result.Error)
		return
	}

	switch result.Operation {
	case "put", "create", "update", "get", "migrate":
		fmt.Fprintf(stdout, "OK operation=%s id=%q path=%q size=%d sha256=%s\n", result.Operation, result.ID, result.Path, result.Info.Size, result.Info.SHA256)
	case "stat":
		checksum := ""
		if result.Info.SHA256 != "" {
			checksum = " sha256=" + result.Info.SHA256
		}
		fmt.Fprintf(stdout, "OK operation=stat id=%q size=%d mod_time=%s%s verified=%t\n", result.ID, result.Info.Size, result.Info.ModTime.UTC().Format(time.RFC3339Nano), checksum, result.Info.Verified)
	case "path":
		fmt.Fprintf(stdout, "%s\n", result.Path)
	default:
		fmt.Fprintf(stdout, "OK operation=%s id=%q\n", result.Operation, result.ID)
	}
}

func printSingleError(jsonOutput bool, operation, id, path string, err error, stdout, stderr io.Writer) {
	printResult(jsonOutput, commandResult{Operation: operation, ID: id, Path: path, Error: err.Error(), StateChanged: errors.Is(err, storage.ErrCommitUncertain)}, stdout, stderr)
}

func printUsage(w io.Writer) {
	commands := []string{
		"cleanup     remove old orphaned temporary files when object locks are free",
		"completion  generate shell completion scripts (bash, zsh, fish)",
		"create      create only; fail if an object already exists",
		"delete      delete one or more objects",
		"get         atomically download one or more objects",
		"migrate     wrap legacy objects: <id> <trusted-sha256> pairs",
		"path        print the internal sharded path for an object ID",
		"put         upsert one or more objects",
		"stat        inspect object size/time and optionally SHA-256",
		"update      update only; fail if an object does not exist",
		"version     print version information",
	}
	sort.Strings(commands)

	fmt.Fprintln(w, "storage-cli - sharded local object storage")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  storage-cli <command> [flags] [arguments]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, command := range commands {
		fmt.Fprintf(w, "  %s\n", command)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Write/get arguments are explicit pairs:")
	fmt.Fprintln(w, "  storage-cli put    --store ./data invoice:123 ./invoice.pdf avatar:42 ./avatar.png")
	fmt.Fprintln(w, "  storage-cli create --store ./data immutable:key ./payload.bin")
	fmt.Fprintln(w, "  storage-cli update --store ./data invoice:123 ./replacement.pdf")
	fmt.Fprintln(w, "  storage-cli get    --store ./data invoice:123 ./downloads/invoice.pdf")
	fmt.Fprintln(w, "  storage-cli get    --overwrite --store ./data invoice:123 ./downloads/invoice.pdf")
	fmt.Fprintln(w, "  storage-cli delete --store ./data invoice:123 avatar:42")
	fmt.Fprintln(w, "  storage-cli stat   --checksum --store ./data invoice:123")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Short flags are supported (-s, -w, -j, -t, -o, -m, -c) and env var TB_STORE.")
	fmt.Fprintln(w, "Compatibility aliases: upload=put, download=get")
}

const bashCompletionScript = `_storage_cli() {
    local cur prev words cword
    _init_completion || return

    local commands="put upload create update migrate get download delete stat path cleanup version completion help"
    local common_flags="--store -s --workers -w --json -j --timeout -t --max-bytes --require-durable --help -h"

    if [ $cword -eq 1 ]; then
        COMPREPLY=( $(compgen -W "${commands}" -- "$cur") )
        return 0
    fi

    local command="${words[1]}"
    case "$command" in
        put|upload|create|update|migrate)
            COMPREPLY=( $(compgen -W "${common_flags}" -- "$cur") )
            ;;
        get|download)
            COMPREPLY=( $(compgen -W "${common_flags} --overwrite -o" -- "$cur") )
            ;;
        delete)
            COMPREPLY=( $(compgen -W "${common_flags} --missing-ok -m" -- "$cur") )
            ;;
        stat)
            COMPREPLY=( $(compgen -W "${common_flags} --checksum -c" -- "$cur") )
            ;;
        path)
            COMPREPLY=( $(compgen -W "--store -s --json -j --help -h" -- "$cur") )
            ;;
        cleanup)
            COMPREPLY=( $(compgen -W "--store -s --older-than --json -j --downloads --help -h" -- "$cur") )
            ;;
        completion)
            COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") )
            ;;
    esac
}
complete -F _storage_cli storage-cli
`

const zshCompletionScript = `#compdef storage-cli

_storage_cli() {
    local -a commands
    commands=(
        'put:upsert one or more objects'
        'upload:alias for put'
        'create:create only; fail if an object already exists'
        'update:update only; fail if an object does not exist'
        'get:atomically download one or more objects'
        'download:alias for get'
        'delete:delete one or more objects'
        'stat:inspect object size/time and optionally SHA-256'
        'path:print internal sharded path for an object ID'
        'migrate:wrap legacy objects: <id> <trusted-sha256> pairs'
        'cleanup:remove old orphaned temporary files'
        'completion:generate shell completion script'
        'version:print version information'
        'help:print help'
    )

    if (( CURRENT == 2 )); then
        _describe 'command' commands
        return
    fi

    case "$words[2]" in
        put|upload|create|update|migrate)
            _arguments \
                '(-s --store)'{-s,--store}'[storage directory]:directory:_files -/' \
                '(-w --workers)'{-w,--workers}'[maximum concurrent operations]:number:' \
                '(-j --json)'{-j,--json}'[emit newline-delimited JSON]' \
                '(-t --timeout)'{-t,--timeout}'[operation timeout]:duration:' \
                '--max-bytes[maximum object payload bytes]:bytes:' \
                '--require-durable[reject platforms without directory durability]' \
                '*:arguments:_files'
            ;;
        get|download)
            _arguments \
                '(-s --store)'{-s,--store}'[storage directory]:directory:_files -/' \
                '(-w --workers)'{-w,--workers}'[maximum concurrent operations]:number:' \
                '(-j --json)'{-j,--json}'[emit newline-delimited JSON]' \
                '(-t --timeout)'{-t,--timeout}'[operation timeout]:duration:' \
                '(-o --overwrite)'{-o,--overwrite}'[atomically replace existing destination]' \
                '--max-bytes[maximum object payload bytes]:bytes:' \
                '--require-durable[reject platforms without directory durability]' \
                '*:arguments:_files'
            ;;
        delete)
            _arguments \
                '(-s --store)'{-s,--store}'[storage directory]:directory:_files -/' \
                '(-w --workers)'{-w,--workers}'[maximum concurrent operations]:number:' \
                '(-j --json)'{-j,--json}'[emit newline-delimited JSON]' \
                '(-m --missing-ok)'{-m,--missing-ok}'[do not fail when object does not exist]' \
                '*:object IDs:'
            ;;
        stat)
            _arguments \
                '(-s --store)'{-s,--store}'[storage directory]:directory:_files -/' \
                '(-w --workers)'{-w,--workers}'[maximum concurrent operations]:number:' \
                '(-j --json)'{-j,--json}'[emit newline-delimited JSON]' \
                '(-c --checksum)'{-c,--checksum}'[verify SHA-256 checksum]' \
                '*:object IDs:'
            ;;
        path)
            _arguments \
                '(-s --store)'{-s,--store}'[storage directory]:directory:_files -/' \
                '(-j --json)'{-j,--json}'[emit newline-delimited JSON]' \
                '*:object IDs:'
            ;;
        cleanup)
            _arguments \
                '(-s --store)'{-s,--store}'[storage directory]:directory:_files -/' \
                '(-j --json)'{-j,--json}'[emit newline-delimited JSON]' \
                '--older-than[orphan age threshold]:duration:' \
                '--downloads[clean download directory]:directory:_files -/'
            ;;
        completion)
            _values 'shell' 'bash' 'zsh' 'fish'
            ;;
    esac
}

_storage_cli "$@"
`

const fishCompletionScript = `# fish completion for storage-cli
complete -c storage-cli -f

complete -c storage-cli -n "__fish_use_subcommand" -a put -d "upsert one or more objects"
complete -c storage-cli -n "__fish_use_subcommand" -a upload -d "alias for put"
complete -c storage-cli -n "__fish_use_subcommand" -a create -d "create only; fail if exists"
complete -c storage-cli -n "__fish_use_subcommand" -a update -d "update only; fail if missing"
complete -c storage-cli -n "__fish_use_subcommand" -a get -d "atomically download objects"
complete -c storage-cli -n "__fish_use_subcommand" -a download -d "alias for get"
complete -c storage-cli -n "__fish_use_subcommand" -a delete -d "delete one or more objects"
complete -c storage-cli -n "__fish_use_subcommand" -a stat -d "inspect object metadata/checksum"
complete -c storage-cli -n "__fish_use_subcommand" -a path -d "print internal sharded path"
complete -c storage-cli -n "__fish_use_subcommand" -a migrate -d "wrap legacy objects"
complete -c storage-cli -n "__fish_use_subcommand" -a cleanup -d "remove old staging files"
complete -c storage-cli -n "__fish_use_subcommand" -a completion -d "generate shell completion"
complete -c storage-cli -n "__fish_use_subcommand" -a version -d "print version"
complete -c storage-cli -n "__fish_use_subcommand" -a help -d "print help"

complete -c storage-cli -s s -l store -d "storage directory" -r
complete -c storage-cli -s w -l workers -d "concurrent operations" -r
complete -c storage-cli -s j -l json -d "emit NDJSON"
complete -c storage-cli -s t -l timeout -d "batch timeout" -r
complete -c storage-cli -l max-bytes -d "max object payload bytes" -r
complete -c storage-cli -l require-durable -d "require directory fsync"
complete -c storage-cli -s o -l overwrite -d "replace existing destination"
complete -c storage-cli -s m -l missing-ok -d "ignore missing objects"
complete -c storage-cli -s c -l checksum -d "verify SHA-256"
`
