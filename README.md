# Thunderbolt Storage

Thunderbolt Storage is a local filesystem object store and CLI written in Go.
It provides atomic per-object publication, persisted SHA-256 integrity metadata,
cooperative process locking, bounded batches, and recovery of abandoned staging
files. Production dependencies are limited to the Go standard library.

The supported interface is the CLI. Packages under `internal` are implementation
APIs, not a public Go library. Go 1.25 or newer is required. Linux, macOS, and
Windows adapters are included; filesystem and power-loss behavior must be
qualified on the actual deployment platform.

**Disk format change:** objects now contain an 80-byte `TBSTORE1` header followed
by the original payload. Existing raw objects need explicit migration. Do not
run old and new binaries against the same store. See [Migration](#migration).

The module path `example.com/robust-storage-cli` remains a development placeholder.
Replace it and the internal imports before publishing a reusable module.

## Quick start

```sh
make build VERSION=1.0.0
printf 'hello\n' > greeting.txt
./storage-cli put --store ./data greeting ./greeting.txt
./storage-cli get --store ./data greeting ./downloads/greeting.txt
cmp greeting.txt downloads/greeting.txt
./storage-cli stat --checksum --store ./data greeting
```

The default store is `./local_store`. A new store and its internal directories
are created with mode `0700`; object and download staging files use `0600` on
Unix. Existing Unix store roots must grant no group/other access. Windows uses
ACLs: provision a private store and private destination directories yourself.

## Commands

Flags precede positional arguments. Use `--` before IDs beginning with `-`.

| Command | Arguments | Behavior |
| --- | --- | --- |
| `put` / `upload` | `<id> <source> ...` | Create or replace each object. |
| `create` | `<id> <source> ...` | Fail if an object already exists. |
| `update` | `<id> <source> ...` | Fail if an object does not exist. |
| `get` / `download` | `<id> <destination> ...` | Verify and atomically download each payload. |
| `delete` | `<id> ...` | Delete each object. |
| `stat` | `<id> ...` | Read stored size, checksum, and modification time. |
| `path` | `<id> ...` | Print internal container paths; existence is not checked. |
| `migrate` | `<id> <trusted-sha256> ...` | Verify and atomically wrap legacy raw objects. |
| `cleanup` | none | Reclaim old, recognized staging files. |
| `version` | none | Print version, commit, build date, and platform. |

`put`, `create`, `update`, `get`, `delete`, `stat`, and `migrate` support:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--store` | `./local_store` | Store directory. |
| `--workers` | CPU count, clamped to 2–8 | Concurrent operations, from 1 to 256. |
| `--json` | false | Emit newline-delimited JSON (NDJSON). |
| `--timeout` | `0` | Per-command batch timeout; `0` disables it. |
| `--max-bytes` | `0` | Maximum payload size for writes/migration; `0` is unlimited. Ignored by read/delete operations. |
| `--require-durable` | false | Reject platforms without directory durability barriers. |

`--require-durable` is appropriate for Unix deployments requiring durability.
It rejects Windows and unsupported adapters instead of silently promising Unix
crash semantics. It does not certify a filesystem, controller, or physical disk.

Additional flags:

| Command | Flag | Meaning |
| --- | --- | --- |
| `get` | `--overwrite` | Replace an existing regular destination. Default: preserve it and fail. |
| `delete` | `--missing-ok` | Accept an absent object. |
| `stat` | `--checksum` | Read the complete payload and compare it to the persisted checksum. |
| `cleanup` | `--older-than` | Positive duration, default `24h`. |
| `cleanup` | `--downloads <directory>` | Clean download fragments in this one directory instead of object staging files. |

`path` supports `--store` and `--json`. `cleanup` supports `--store`, `--json`,
`--older-than`, and `--downloads`.

Examples:

```sh
storage-cli put --store ./data --workers 4 --max-bytes 1073741824 \
  --timeout 5m --require-durable invoice:123 ./invoice.pdf avatar:42 ./avatar.png
storage-cli get --overwrite --store ./data invoice:123 ./downloads/invoice.pdf
storage-cli stat --json --checksum --store ./data invoice:123
storage-cli cleanup --store ./data --older-than 24h
storage-cli cleanup --store ./data --downloads ./downloads --older-than 24h
```

Object IDs contain 1–16,384 bytes and no NUL. They are opaque: an ID such as
`../../not-a-path` is hashed before a storage path is constructed. Sources must
be regular files; source symlinks and special files are rejected. Unix source
opens use `O_NONBLOCK` and `O_NOFOLLOW`, so a FIFO cannot hang before validation.

## Output and failures

Successful human-readable records go to stdout; per-item errors go to stderr.
With `--json`, both successful and failed item records go to stdout. Batches
finish their submitted work and return records in input order. They are not
transactions: one item can succeed while another fails. Results are buffered
until the batch completes, so NDJSON is a record format, not live progress.

```json
{"operation":"get","id":"invoice:123","path":"./invoice.pdf","info":{"id":"invoice:123","size":1234,"sha256":"…","verified":true,"modTime":"2026-09-28T12:00:00Z"}}
```

`stat` always reports the persisted checksum. `verified` is false unless the
payload was read and checked with `--checksum`. Successful writes, downloads,
and migration return `verified: true`. Metadata-only stat validates the header,
ID binding, and length, but cannot detect same-length payload corruption.

Cleanup returns one report such as:

```json
{"scanned":3,"removed":1,"skipped":2}
```

| Exit | Meaning |
| --- | --- |
| `0` | All requested operations and output writes succeeded. |
| `1` | Storage, integrity, initialization, or output failure. |
| `2` | Invalid command-line usage. |
| `130` | Cancellation or batch deadline expiration. |

A directory sync can fail **after** a publication or deletion is visible.
Such errors wrap `storage.ErrCommitUncertain`, use `*storage.CommitError`, and
include `"stateChanged": true` in JSON error records. Inspect the object before
retrying. In particular, retrying `create` can return “already exists”; retrying
delete can return “not found.” The API returns committed object information
alongside a publication durability error when available.

An output failure also cannot undo completed storage mutations. Likewise, a
cancelled batch may have completed some items before cancellation. Error text
is for diagnostics; Go callers should use `errors.Is`/`errors.As` for classification.

## Storage layout and integrity

```text
<store>/
  objects/
    ab/
      cd/
        <64-character SHA-256 of logical ID>
  locks/
    ab/
      c.lock
```

The 80-byte version-1 header contains:

| Bytes | Value |
| --- | --- |
| 0–7 | ASCII `TBSTORE1` format identifier |
| 8–15 | Unsigned big-endian payload length, constrained to a nonnegative int64 |
| 16–47 | SHA-256 of the payload |
| 48–79 | SHA-256 of the logical ID |
| 80 onward | Original file payload |

The header and payload are staged in the same file, synced, and published in
one operation. There is no independently committed checksum sidecar. Downloads
validate length, ID binding, and checksum before replacing the destination.
Truncated objects, extra bytes, unsupported headers, and checksum mismatches
return `ErrIntegrity`; an existing destination remains intact.

Checksums detect accidental corruption, not malicious changes by someone with
write access to both data and metadata. This is not encryption or authentication.
The original logical ID is not recoverable from its hash: retain an external ID
catalog and trusted checksum manifest if you need enumeration and independent
recovery verification. `path` points to a container, not a directly usable payload.

## Atomicity, locking, and durability

- Writes stage in the target directory, flush file contents, and publish by
  rename/replacement. Create-only publication uses a hard link to combine
  publication and the no-overwrite check atomically.
- Directory creation synchronizes the directory hierarchy, not just the leaf.
  Unix mutations also sync the containing directory after publication/deletion.
- There are 4,096 stable lock stripes. Writers use exclusive locks; reads use
  shared locks. Hash collisions cause contention, not incorrect object identity.
  Lock files must never be removed or replaced during operation.
- Downloads stage beside their destination and verify content before committing.
  Destinations inside the canonical store, including parent-symlink aliases,
  are rejected. Final-component symlinks and special files are rejected.
- Batches reject duplicate write IDs and canonicalized, case-folded download
  destinations. Case folding is conservative and can reject distinct paths
  on a case-sensitive filesystem. Avoid filesystem-specific Unicode aliases
  and alternate names; these are not a portable destination naming scheme.
- Separate commands downloading to the same external destination are not
  serialized. Without overwrite, one publication wins; with overwrite, the
  last successful publication wins. Coordinate such workflows externally.

Store roots, all parents, source files during upload, and destination directories
must remain controlled by trusted cooperating users. Path validation is not a
sandbox against a process concurrently replacing parent directories. Do not
mutate the store through other tools while it is active. Preexisting internal
symlink directories are rejected.

| Platform | Behavior |
| --- | --- |
| Linux/macOS/supported BSDs | `flock`, rename/hard-link publication, file and directory syncing. Qualify actual filesystem and hardware power-loss behavior. |
| Windows | `LockFileEx`, `ReplaceFileW`, and `MoveFileExW(WRITE_THROUGH)` for new replacement targets. File data is flushed, but directory syncing is unavailable in this adapter; creation by hard link, replacement, and deletion do not claim equivalent power-loss durability. `--require-durable` rejects this mode. |
| Other platforms | Cross-process locking fails explicitly; storage operations are unsupported. |

Use a qualified local filesystem supporting hard links, locks, and atomic
replacement. NFS, SMB, and distributed filesystems are outside the tested
contract. A passing unit suite or race detector cannot certify power-loss safety.

## Cancellation and capacity

SIGINT and SIGTERM cancel work on Unix; Windows supports `os.Interrupt`.
The first signal requests cancellation; signal handling is then restored so a
second signal can force termination. Lock retries check cancellation, and copies
check it between reads and again before publication. The timeout applies to
batch work; filesystem initialization and individual blocked OS calls can exceed
it. It is not a hard wall-clock kill deadline.

`Store.Put` accepts an arbitrary `io.Reader`. Callers must arrange for a blocked
reader to unblock through its own deadline, cancellation, or close mechanism.
The store does not launch abandoned goroutines to simulate cancellation.

Each copying worker uses approximately a 1 MiB buffer plus normal runtime
allocations. Results require O(batch size) memory. `--max-bytes` limits each
staged upload, including streaming readers. It is not a total store quota.
Monitor free bytes and inodes and reserve room for the old object, replacement,
concurrent staging files, downloads, and backups. A failed stage leaves the
previous object intact; sync failures after publication have uncertain durability.

## Cleanup and recovery

`cleanup` removes only recognized, expired regular staging files in the correct
object shard. It acquires the matching exclusive stripe lock and checks the file
again under that lock. Active writers/downloads are skipped. Symlinks, unexpected
names, and recently modified files are retained. Concurrent disappearance of a
walk entry is tolerated.

Download fragments include the canonical store-path hash and source-object hash.
`cleanup --downloads DIR` scans only DIR, not its subdirectories, and only names
belonging to this store. Active downloads hold the source read lock, preventing
cleanup from removing their files. Run this command for each download directory.
Moving the store changes its download-prefix identity: old fragments then need
manual inspection and removal while all relevant processes are stopped.

After an unclean process exit:

1. Restart using the same store and permissions. OS locks are released on exit.
2. Verify important IDs with `stat --checksum` and compare external manifests.
3. Run age-qualified cleanup for the store and download directories.
4. On integrity failure, preserve the damaged container for investigation and
   restore from a verified backup. Do not overwrite the only surviving copy.

## Migration

This release deliberately rejects legacy raw objects during get/stat. No
ambiguous fallback treats a damaged container header as an unchecked raw payload.
Before migration, stop all old binaries and take a quiescent backup.

For each known ID, supply the **trusted SHA-256 of the original payload**:

```sh
storage-cli migrate --store ./data --json \
  invoice:123 "$TRUSTED_SHA256"
storage-cli stat --checksum --store ./data invoice:123
```

Set `TRUSTED_SHA256` to the actual 64-character digest from your trusted manifest before running the command.
A checksum mismatch preserves the old object. Successful migration is atomic per
object and idempotent: repeating the command verifies an already migrated object.
Batch migration can partially succeed, and the same uncertain-durability error
contract applies. A failed migration can be retried after inspection.

Do not derive a new checksum from suspected corrupted data and treat it as proof
of integrity. If no trusted manifest exists, recover known source files and
re-upload them, or explicitly verify legacy data by an independent process first.
Do not roll back to an old binary after migrating unless you restore the matching
pre-migration backup.

## Backup and restore

There is no multi-object snapshot transaction, replication, or built-in ID index.
Stop writers or take an application-coordinated filesystem snapshot. Capture
committed containers together with the external ID catalog and trusted checksum
manifest at a consistent point. Staging files are not committed objects.

Example on Unix, with all storage processes stopped:

```sh
tar -cpf store-backup.tar -C ./data .
mkdir -m 700 ./restored
tar -xpf store-backup.tar -C ./restored
storage-cli stat --checksum --store ./restored invoice:123
storage-cli get --store ./restored invoice:123 ./restore-check.pdf
```

Verify **every expected ID**, size, and checksum against the independent manifest;
spot-checking one ID is only a smoke test. Keep backups on independent storage.
Record and rehearse your recovery time and recovery point objectives before
using the store as the authoritative copy. Tests include a quiescent container
copy/restore round trip; deployment backup tooling still needs its own drill.

## Architecture and internal API

```text
cmd/storage-cli       flags, validation, checked output, signals, exit codes
       |
       +-- internal/batch     bounded workers and ordered per-item results
       |
       +-- internal/storage  versioned containers, staged publication,
                             locks, paths, migration, cleanup, OS adapters
```

`storage.NewWithOptions(path, Options{MaxObjectBytes: n, RequireDurability: true})`
configures policy once. Store instances support concurrent operations; their
configuration is immutable. `New` uses unlimited payload sizes and platform
best-effort durability policy. Public methods are `BaseDir`, `ObjectPath`,
`PutFile`, `Put`, `GetFile`, `Delete`, `Stat`, `Migrate`, `CleanupTemps`, and
`CleanupDownloads`.

Error sentinels include `ErrNotFound`, `ErrAlreadyExists`, `ErrInvalidID`,
`ErrInsecurePermissions`, `ErrNotRegularFile`, `ErrIntegrity`, `ErrTooLarge`,
`ErrUnsafeDestination`, and `ErrCommitUncertain`. `ObjectInfo` includes `ID`,
payload `Size`, `SHA256`, `Verified`, and `ModTime`. `StorePath` remains reserved;
use `ObjectPath` when an internal path is needed.

## Development and quality gates

```sh
make fmt                  # intentionally rewrite formatting
make check                # read-only format check, vet, race+coverage, build
make test                 # ordinary tests
make race                 # race detector
make coverage             # report + minimum 90% aggregate statement coverage
make fuzz FUZZTIME=15s     # bounded fuzz campaigns for IDs, temp names, CLI pairs
make bench                # payload-size write benchmarks and allocation counts
make vuln                 # pinned govulncheck; requires network access
```

`make check` does not rewrite source. Build metadata accepts `VERSION`, `COMMIT`,
and `BUILD_DATE`. The test timeout is three minutes. Windows CI uses an 85%
aggregate coverage floor because additional Windows adapters are compiled;
Linux/macOS use 90%. `COVERAGE_MIN` can override the local Makefile threshold.

The GitHub Actions workflow runs real tests on Linux, macOS, and Windows, using
both Go 1.25 and the stable toolchain. It checks formatting, vet, race detection,
coverage, and compilation. A separate job runs fuzzing and vulnerability checks,
including on a weekly schedule. The vulnerability scanner is a development tool;
it does not add a runtime module dependency. Its behavior is described in the
[official Go vulnerability documentation](https://go.dev/doc/security/vuln/).

Tests cover command success/failure/JSON paths, bounded and cancelled batches,
corrupt/truncated/wrong-ID objects, failed input and sync/commit boundaries,
size limits, destination aliases, FIFOs on Unix, migration, backup restore,
active/orphan cleanup, cross-process create-only races, and killed-writer recovery.

Local verification snapshot (Go 1.27.1, macOS/amd64):

| Package | Statement coverage |
| --- | --- |
| CLI | 97.2% |
| Batch executor | 100.0% |
| Storage | 90.6% |
| Aggregate | 93.6% |

`make check`, three short fuzz campaigns, the vulnerability scan, and Linux/
Windows cross-compilation passed locally. The built CLI also passed FIFO rejection
and SIGTERM cancellation during lock contention. Cross-platform runtime jobs are
configured in CI; their remote results must still be checked after pushing.

Coverage is a regression signal, not proof that every filesystem failure is
simulated. Remaining OS-dependent failures and real power-loss behavior require
platform qualification. Cross-compilation alone does not replace runtime CI.
Before release, ensure all implementation and workflow files are committed,
run the quality gates, inspect CI results, benchmark the intended workload, and
complete a deployment-specific recovery drill.
