#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'WSL sandbox acceptance failed: %s\n' "$*" >&2
  exit 1
}

# WSL2 may expose a Windows drive as a 9p transport while retaining the
# DrvFS backend identity as `aname=drvfs;...` in the mount options. Accept
# only that exact value boundary: a generic 9p mount and `drvfs-evil` remain
# outside the acceptance environment.
is_drvfs_mount() {
  local filesystem=$1 options=$2
  if [[ "$filesystem" == 'drvfs' ]]; then
    return 0
  fi
  [[ "$filesystem" == '9p' ]] || return 1
  case ",$options," in
    *,aname=drvfs\;*|*,aname=drvfs,*) return 0 ;;
    *) return 1 ;;
  esac
}

distro=''
user=''
ext4_root=''
windows_root=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --distro) distro=${2:-}; shift 2 ;;
    --user) user=${2:-}; shift 2 ;;
    --ext4-root) ext4_root=${2:-}; shift 2 ;;
    --windows-root) windows_root=${2:-}; shift 2 ;;
    *) fail "unknown argument $1" ;;
  esac
done

[[ "$distro" == 'Ubuntu-24.04' ]] || fail 'Distro must be Ubuntu-24.04'
[[ "$user" != '' ]] || fail 'User is required'
[[ -n "${WSL_DISTRO_NAME:-}" && "$WSL_DISTRO_NAME" == "$distro" ]] || fail 'WSL distribution identity mismatch'
[[ "$(id -u)" != '0' ]] || fail 'root is not an acceptance environment; provide a non-root user'
[[ -d "$ext4_root" ]] || fail 'Ext4Root must exist'
[[ -d "$windows_root" ]] || fail 'WindowsRoot must exist'

source /etc/os-release
[[ "${ID:-}" == 'ubuntu' && "${VERSION_ID:-}" == '24.04' ]] || fail 'Ubuntu 24.04 is required'

for command in bwrap prlimit go findmnt; do
  command -v "$command" >/dev/null 2>&1 || fail "missing required command $command"
done
[[ -r /proc/sys/user/max_user_namespaces ]] || fail 'user namespaces are unavailable'
[[ "$(cat /proc/sys/user/max_user_namespaces)" -gt 0 ]] || fail 'user namespaces are disabled'

ext4_type=$(findmnt -T "$ext4_root" -no FSTYPE)
[[ "$ext4_type" == 'ext4' ]] || fail "Ext4Root must be ext4, got $ext4_type"
windows_type=$(findmnt -T "$windows_root" -no FSTYPE)
windows_options=$(findmnt -T "$windows_root" -no OPTIONS)
if ! is_drvfs_mount "$windows_type" "$windows_options"; then
  fail "WindowsRoot must be DrvFS (drvfs or 9p aname=drvfs), got type=$windows_type"
fi

# Resolve the already-selected Go 1.25.13 toolchain before isolating module
# state. WSL's launcher can be an older bootstrap Go which finds its selected
# toolchain through the user's normal module cache; moving GOMODCACHE first
# would trigger a new download to the D-backed mount.
selected_go="$(go env GOROOT)/bin/go"
[[ -x "$selected_go" ]] || fail "selected Go toolchain is unavailable at $selected_go"
selected_go_version="$("$selected_go" version)"
case "$selected_go_version" in
  'go version go1.25.13 '*) ;;
  *) fail "Go 1.25.13 is required, got $selected_go_version" ;;
esac

umask 077
run_id=$(basename "$windows_root")
ext4_run=$(mktemp -d "$ext4_root/harness-wsl-sandbox.XXXXXX")
cache_root="$ext4_run/cache"
tmp_root="$ext4_run/tmp"
work_root="$windows_root/work"
mkdir -p "$cache_root/go-build" "$cache_root/go-mod" "$tmp_root" "$work_root"

remove_test_tree() {
  local target=$1
  [[ -e "$target" ]] || return 0

  # The Go module cache marks extracted toolchain files read-only.  Make the
  # dedicated test tree writable before removal so a successful run cannot
  # leave D-backed cache data behind merely because of those mode bits.
  chmod -R u+w -- "$target" 2>/dev/null || true
  rm -rf -- "$target"
}

cleanup() {
  status=$?
  case "$ext4_run" in
    "$ext4_root"/harness-wsl-sandbox.*)
      remove_test_tree "$ext4_run" || status=1
      ;;
    *) printf 'refusing to remove unexpected ext4 run root\n' >&2; status=1 ;;
  esac
  remove_test_tree "$cache_root" || status=1
  remove_test_tree "$tmp_root" || status=1
  remove_test_tree "$work_root" || status=1
  exit "$status"
}
trap cleanup EXIT

export GOCACHE="$cache_root/go-build"
export GOMODCACHE="$cache_root/go-mod"
export GOTMPDIR="$tmp_root"
export TMPDIR="$tmp_root"
export GOTOOLCHAIN='local'
export HARNESS_WSL_INTEGRATION=1
export HARNESS_WSL_EXT4_ROOT="$ext4_run"
export HARNESS_WSL_WINDOWS_ROOT="$work_root"
export HARNESS_WSL_EVIDENCE_PATH="$windows_root/performance.json"

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repo_root"

"$selected_go" test -count=1 -timeout 90s -run '^TestWSLLiveProbe$' ./pkg/execution/sandbox
"$selected_go" test -count=3 -timeout 360s -run '^TestWSL(LiveProbe|LocalProvider.*)$' ./pkg/execution/sandbox
printf '{"schema":"harness-wsl-sandbox-v1","run_id":"%s","distro":"Ubuntu-24.04","user":"%s","correctness_count":3,"status":"pass"}\n' "$run_id" "$user" > "$windows_root/correctness.json"

export HARNESS_WSL_PERFORMANCE=1
"$selected_go" test -count=1 -timeout 360s -run '^TestWSLPerformanceEvidence$' ./pkg/execution/sandbox
