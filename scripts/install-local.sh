#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL_SOURCE_ROOT="${ROOT_DIR}/assets/codex-skills"
LOCAL_ROOT="${1:-$HOME/.local}"
SKILL_DEST_ROOT="${2:-$HOME/.codex/skills}"
LOCAL_BIN_DIR="${LOCAL_ROOT}/bin"

install_skill_link() {
  local name="$1"
  local source_dir="${SKILL_SOURCE_ROOT}/${name}"
  local dest_dir="${SKILL_DEST_ROOT}/${name}"
  mkdir -p "${SKILL_DEST_ROOT}"
  rm -rf "${dest_dir}"
  ln -s "${source_dir}" "${dest_dir}"
  printf 'installed skill symlink: %s -> %s\n' "${dest_dir}" "${source_dir}"
}

mkdir -p "${LOCAL_BIN_DIR}"

cargo build --release -p fidget-spinner-cli --manifest-path "${ROOT_DIR}/Cargo.toml"
install -m 0755 \
  "${ROOT_DIR}/target/release/fidget-spinner-cli" \
  "${LOCAL_BIN_DIR}/fidget-spinner-cli"

printf 'installed binary: %s\n' "${LOCAL_BIN_DIR}/fidget-spinner-cli"

install_skill_link "fidget-spinner"
install_skill_link "frontier-loop"

printf 'mcp command: %s\n' "${LOCAL_BIN_DIR}/fidget-spinner-cli mcp serve"
