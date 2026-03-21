#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL_SOURCE_ROOT="${ROOT_DIR}/assets/codex-skills"
SYSTEMD_TEMPLATE_ROOT="${ROOT_DIR}/assets/systemd"
LOCAL_ROOT="${1:-$HOME/.local}"
SKILL_DEST_ROOT="${2:-$HOME/.codex/skills}"
LOCAL_BIN_DIR="${LOCAL_ROOT}/bin"
SYSTEMD_USER_DIR="${HOME}/.config/systemd/user"
UI_SERVICE_NAME="${FIDGET_SPINNER_UI_SERVICE_NAME:-fidget-spinner-libgrid-ui.service}"
UI_PROJECT_ROOT="${FIDGET_SPINNER_UI_PROJECT:-$HOME/programming/projects/libgrid/.worktrees/libgrid-lp-oracle-cutset}"
UI_BIND="${FIDGET_SPINNER_UI_BIND:-127.0.0.1:8913}"

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\\/&]/\\&/g'
}

install_skill_link() {
  local name="$1"
  local source_dir="${SKILL_SOURCE_ROOT}/${name}"
  local dest_dir="${SKILL_DEST_ROOT}/${name}"
  mkdir -p "${SKILL_DEST_ROOT}"
  rm -rf "${dest_dir}"
  ln -s "${source_dir}" "${dest_dir}"
  printf 'installed skill symlink: %s -> %s\n' "${dest_dir}" "${source_dir}"
}

listener_pid_for_bind() {
  local bind="$1"
  local port="${bind##*:}"
  ss -ltnp "( sport = :${port} )" 2>/dev/null \
    | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' \
    | head -n 1
}

evict_conflicting_navigator() {
  local pid
  pid="$(listener_pid_for_bind "${UI_BIND}")"
  if [[ -z "${pid}" ]]; then
    return 0
  fi
  local cmd
  cmd="$(ps -p "${pid}" -o args= || true)"
  if [[ "${cmd}" == *"fidget-spinner-cli ui serve"* ]]; then
    kill "${pid}"
    for _ in {1..20}; do
      if ! kill -0 "${pid}" 2>/dev/null; then
        printf 'stopped conflicting navigator process: pid=%s\n' "${pid}"
        return 0
      fi
      sleep 0.1
    done
    printf 'failed to stop conflicting navigator process: pid=%s\n' "${pid}" >&2
    return 1
  fi
  printf 'refusing to steal %s from non-spinner process: %s\n' "${UI_BIND}" "${cmd}" >&2
  return 1
}

install_libgrid_ui_service() {
  if [[ ! -d "${UI_PROJECT_ROOT}" ]]; then
    printf 'libgrid navigator root does not exist: %s\n' "${UI_PROJECT_ROOT}" >&2
    return 1
  fi
  if ! command -v systemctl >/dev/null 2>&1; then
    printf 'systemctl unavailable; skipping navigator service install\n' >&2
    return 0
  fi

  local service_path="${SYSTEMD_USER_DIR}/${UI_SERVICE_NAME}"
  local template_path="${SYSTEMD_TEMPLATE_ROOT}/${UI_SERVICE_NAME}.in"
  mkdir -p "${SYSTEMD_USER_DIR}"
  sed \
    -e "s|@HOME@|$(escape_sed_replacement "${HOME}")|g" \
    -e "s|@LOCAL_BIN_DIR@|$(escape_sed_replacement "${LOCAL_BIN_DIR}")|g" \
    -e "s|@UI_PROJECT_ROOT@|$(escape_sed_replacement "${UI_PROJECT_ROOT}")|g" \
    -e "s|@UI_BIND@|$(escape_sed_replacement "${UI_BIND}")|g" \
    "${template_path}" > "${service_path}"
  chmod 0644 "${service_path}"
  printf 'installed user service: %s\n' "${service_path}"

  export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
  if [[ -z "${DBUS_SESSION_BUS_ADDRESS:-}" && -S "${XDG_RUNTIME_DIR}/bus" ]]; then
    export DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"
  fi
  if ! systemctl --user daemon-reload; then
    printf 'systemd user manager unavailable; skipping navigator service activation\n' >&2
    return 0
  fi
  evict_conflicting_navigator
  if systemctl --user is-enabled --quiet "${UI_SERVICE_NAME}"; then
    systemctl --user restart "${UI_SERVICE_NAME}"
    printf 'restarted user service: %s\n' "${UI_SERVICE_NAME}"
  else
    systemctl --user enable --now "${UI_SERVICE_NAME}"
    printf 'enabled user service: %s\n' "${UI_SERVICE_NAME}"
  fi
}

mkdir -p "${LOCAL_BIN_DIR}"

cargo build --release -p fidget-spinner-cli --manifest-path "${ROOT_DIR}/Cargo.toml"
install -m 0755 \
  "${ROOT_DIR}/target/release/fidget-spinner-cli" \
  "${LOCAL_BIN_DIR}/fidget-spinner-cli"

printf 'installed binary: %s\n' "${LOCAL_BIN_DIR}/fidget-spinner-cli"

install_skill_link "fidget-spinner"
install_skill_link "frontier-loop"
install_libgrid_ui_service

printf 'mcp command: %s\n' "${LOCAL_BIN_DIR}/fidget-spinner-cli mcp serve"
