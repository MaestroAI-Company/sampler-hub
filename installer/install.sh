#!/bin/sh
# installs or updates sampler hub for this user on linux and macos
# curl -fsSL https://github.com/MaestroAI-Company/sampler-hub/releases/latest/download/install.sh | sh
set -eu

BASE="https://github.com/MaestroAI-Company/sampler-hub/releases/latest/download"
DIR="${SAMPLER_HUB_DIR:-$HOME/.local/bin}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) ASSET=sampler-hub-linux-x86_64 ;;
  Linux-aarch64 | Linux-arm64) ASSET=sampler-hub-linux-aarch64 ;;
  Darwin-arm64) ASSET=sampler-hub-macos-aarch64 TRAY=sampler-hubw-macos-aarch64 ;;
  *)
    echo "no Sampler-Hub build for $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fetch() {
  curl -fsSL "$BASE/$1" -o "$TMP/$1"
}

# every file must match the release checksums
verify() {
  expected="$(grep " \*\{0,1\}$1\$" "$TMP/SHA256SUMS" | cut -d ' ' -f 1)"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$TMP/$1" | cut -d ' ' -f 1)"
  else
    actual="$(shasum -a 256 "$TMP/$1" | cut -d ' ' -f 1)"
  fi
  if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
    echo "checksum mismatch for $1, nothing installed" >&2
    exit 1
  fi
}

echo "downloading Sampler-Hub"
fetch SHA256SUMS
fetch "$ASSET"
verify "$ASSET"
if [ -n "${TRAY:-}" ]; then
  fetch "$TRAY"
  verify "$TRAY"
fi

mkdir -p "$DIR"
# a running hub keeps its old file open
"$DIR/sampler-hub" service uninstall >/dev/null 2>&1 || true
install -m 755 "$TMP/$ASSET" "$DIR/sampler-hub"
if [ -n "${TRAY:-}" ]; then
  install -m 755 "$TMP/$TRAY" "$DIR/sampler-hubw"
fi
"$DIR/sampler-hub" service install

echo "Sampler-Hub is installed in $DIR and starts with your session"
echo "pair a device with: sampler-hub pair"
