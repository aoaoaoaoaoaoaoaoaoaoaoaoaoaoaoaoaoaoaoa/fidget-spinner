#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="install"
if [[ "${1:-}" == "--uninstall" ]]; then
  MODE="uninstall"
  shift
fi
SKILL_SOURCE_ROOT="${ROOT_DIR}/assets/codex-skills"
SYSTEMD_TEMPLATE_ROOT="${ROOT_DIR}/assets/systemd"
LOCAL_ROOT="${1:-$HOME/.local}"
SKILL_DEST_ROOT="${2:-$HOME/.codex/skills}"
LOCAL_BIN_DIR="${LOCAL_ROOT}/bin"
XDG_CONFIG_ROOT="${XDG_CONFIG_HOME:-}"
if [[ -z "${XDG_CONFIG_ROOT}" || "${XDG_CONFIG_ROOT}" != /* ]]; then
  XDG_CONFIG_ROOT="${HOME}/.config"
fi
SYSTEMD_USER_DIR="${XDG_CONFIG_ROOT}/systemd/user"
UI_SERVICE_NAME="${FIDGET_SPINNER_UI_SERVICE_NAME:-fidget-spinner-ui.service}"
LEGACY_UI_SERVICE_NAME="fidget-spinner-libgrid-ui.service"
UI_BIND="${FIDGET_SPINNER_UI_BIND:-127.0.0.1:8913}"
INSTALL_SYSTEMD="${FIDGET_SPINNER_INSTALL_SYSTEMD:-1}"
OWNERSHIP_MARKER="managed by fidget-spinner installer"
SKILL_MARKER_NAME=".fidget-spinner-owned"
BINARY_MARKER="${LOCAL_BIN_DIR}/.fidget-spinner-cli.eternalist-owned"

for path in "${HOME}" "${LOCAL_ROOT}" "${SKILL_DEST_ROOT}" "${SYSTEMD_USER_DIR}"; do
  if [[ "${path}" != /* ]]; then
    printf 'installation paths must be absolute: %s\n' "${path}" >&2
    exit 1
  fi
done
if [[ ! "${UI_SERVICE_NAME}" =~ ^[A-Za-z0-9:_.@-]+\.service$ ]]; then
  printf 'invalid systemd service name: %s\n' "${UI_SERVICE_NAME}" >&2
  exit 1
fi
if [[ "${UI_BIND}" != 127.*:* && "${UI_BIND}" != '[::1]:'* ]]; then
  printf 'the installed navigator service requires a loopback bind: %s\n' "${UI_BIND}" >&2
  exit 1
fi

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\\/&]/\\&/g'
}

escape_systemd_value() {
  if [[ "$1" == *$'\n'* || "$1" == *$'\r'* ]]; then
    printf 'systemd values cannot contain newlines\n' >&2
    return 1
  fi
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/%/%%/g'
}

owned_marker() {
  local marker="$1"
  [[ -f "${marker}" && ! -L "${marker}" ]] \
    && [[ "$(<"${marker}")" == "${OWNERSHIP_MARKER}" ]]
}

owned_service() {
  local path="$1"
  [[ -f "${path}" && ! -L "${path}" ]] || return 1
  head -n 1 "${path}" | grep -Fqx "# ${OWNERSHIP_MARKER}" \
    || grep -Eq '^ExecStart=.*fidget-spinner-cli.*ui serve' "${path}"
}

legacy_owned_skill() {
  local source_dir="$1"
  local dest_dir="$2"
  local -a entries=()
  [[ -d "${dest_dir}" && ! -L "${dest_dir}" ]] || return 1
  mapfile -d '' entries < <(find "${dest_dir}" -mindepth 1 -maxdepth 1 -print0)
  [[ "${#entries[@]}" == 1 && "${entries[0]}" == "${dest_dir}/SKILL.md" ]] \
    && [[ -f "${dest_dir}/SKILL.md" && ! -L "${dest_dir}/SKILL.md" ]] \
    && cmp -s "${source_dir}/SKILL.md" "${dest_dir}/SKILL.md"
}

install_skill() {
  local name="$1"
  local source_dir="${SKILL_SOURCE_ROOT}/${name}"
  local dest_dir="${SKILL_DEST_ROOT}/${name}"
  local marker="${dest_dir}/${SKILL_MARKER_NAME}"
  mkdir -p "${SKILL_DEST_ROOT}"
  if [[ -e "${dest_dir}" || -L "${dest_dir}" ]]; then
    if [[ -L "${dest_dir}" && "$(readlink "${dest_dir}")" == "${source_dir}" ]]; then
      rm -f "${dest_dir}"
      mkdir "${dest_dir}"
    elif [[ ! -d "${dest_dir}" ]] \
      || { ! owned_marker "${marker}" && ! legacy_owned_skill "${source_dir}" "${dest_dir}"; }; then
      printf 'refusing to replace unowned skill path: %s\n' "${dest_dir}" >&2
      return 1
    fi
  else
    mkdir "${dest_dir}"
  fi
  if [[ -L "${dest_dir}/SKILL.md" ]]; then
    printf 'refusing to follow skill-file symlink: %s\n' "${dest_dir}/SKILL.md" >&2
    return 1
  fi
  printf '%s\n' "${OWNERSHIP_MARKER}" > "${marker}"
  chmod 0644 "${marker}"
  install -m 0644 "${source_dir}/SKILL.md" "${dest_dir}/SKILL.md"
  printf 'installed skill: %s\n' "${dest_dir}"
}

uninstall_skill() {
  local name="$1"
  local source_dir="${SKILL_SOURCE_ROOT}/${name}"
  local dest_dir="${SKILL_DEST_ROOT}/${name}"
  if [[ -L "${dest_dir}" && "$(readlink "${dest_dir}")" == "${source_dir}" ]]; then
    rm -f "${dest_dir}"
    printf 'removed legacy skill symlink: %s\n' "${dest_dir}"
  elif [[ -d "${dest_dir}" ]] && owned_marker "${dest_dir}/${SKILL_MARKER_NAME}"; then
    rm -f "${dest_dir}/${SKILL_MARKER_NAME}"
    if [[ -f "${dest_dir}/SKILL.md" && ! -L "${dest_dir}/SKILL.md" ]]; then
      rm -f "${dest_dir}/SKILL.md"
    fi
    if rmdir "${dest_dir}" 2>/dev/null; then
      printf 'removed skill: %s\n' "${dest_dir}"
    else
      printf 'preserved non-installer contents under skill path: %s\n' "${dest_dir}" >&2
    fi
  elif [[ -e "${dest_dir}" || -L "${dest_dir}" ]]; then
    printf 'preserved unowned skill path: %s\n' "${dest_dir}" >&2
  fi
}

install_binary() {
  local binary="${LOCAL_BIN_DIR}/fidget-spinner-cli"
  local staged_binary
  if [[ -e "${binary}" || -L "${binary}" ]] && [[ ! -f "${binary}" || -L "${binary}" ]]; then
    printf 'refusing non-regular binary path: %s\n' "${binary}" >&2
    return 1
  fi
  if [[ -e "${BINARY_MARKER}" || -L "${BINARY_MARKER}" ]]; then
    owned_marker "${BINARY_MARKER}" || {
      printf 'refusing invalid binary ownership marker: %s\n' "${BINARY_MARKER}" >&2
      return 1
    }
  elif [[ -e "${binary}" || -L "${binary}" ]]; then
    if [[ -L "${binary}" || ! -x "${binary}" ]] \
      || [[ "$("${binary}" --version 2>/dev/null || true)" != fidget-spinner-cli\ * ]]; then
      printf 'refusing to replace unowned binary path: %s\n' "${binary}" >&2
      return 1
    fi
  fi
  staged_binary="$(mktemp "${LOCAL_BIN_DIR}/.fidget-spinner-cli.XXXXXX")"
  if ! install -m 0755 \
    "${CARGO_TARGET_DIR}/release/fidget-spinner-cli" \
    "${staged_binary}"; then
    rm -f "${staged_binary}"
    return 1
  fi
  if ! mv -f "${staged_binary}" "${binary}"; then
    rm -f "${staged_binary}"
    return 1
  fi
  printf '%s\n' "${OWNERSHIP_MARKER}" > "${BINARY_MARKER}"
  chmod 0644 "${BINARY_MARKER}"
  printf 'installed binary: %s\n' "${binary}"
}

uninstall_binary() {
  local binary="${LOCAL_BIN_DIR}/fidget-spinner-cli"
  if owned_marker "${BINARY_MARKER}"; then
    rm -f "${binary}" "${BINARY_MARKER}"
    printf 'removed binary: %s\n' "${binary}"
  elif [[ -x "${binary}" && ! -L "${binary}" ]] \
    && [[ "$("${binary}" --version 2>/dev/null || true)" == fidget-spinner-cli\ * ]]; then
    rm -f "${binary}"
    printf 'removed legacy installer binary: %s\n' "${binary}"
  elif [[ -e "${binary}" || -L "${binary}" || -e "${BINARY_MARKER}" || -L "${BINARY_MARKER}" ]]; then
    printf 'preserved unowned binary path: %s\n' "${binary}" >&2
  fi
}

preflight_install() {
  local binary="${LOCAL_BIN_DIR}/fidget-spinner-cli"
  if [[ -e "${BINARY_MARKER}" || -L "${BINARY_MARKER}" ]]; then
    owned_marker "${BINARY_MARKER}" || {
      printf 'refusing invalid binary ownership marker: %s\n' "${BINARY_MARKER}" >&2
      return 1
    }
    if [[ -e "${binary}" || -L "${binary}" ]] && [[ ! -f "${binary}" || -L "${binary}" ]]; then
      printf 'refusing non-regular binary path: %s\n' "${binary}" >&2
      return 1
    fi
  elif [[ -e "${binary}" || -L "${binary}" ]]; then
    if [[ -L "${binary}" || ! -x "${binary}" ]] \
      || [[ "$("${binary}" --version 2>/dev/null || true)" != fidget-spinner-cli\ * ]]; then
      printf 'refusing to replace unowned binary path: %s\n' "${binary}" >&2
      return 1
    fi
  fi

  local name source_dir dest_dir
  for name in fidget-spinner frontier-loop; do
    source_dir="${SKILL_SOURCE_ROOT}/${name}"
    dest_dir="${SKILL_DEST_ROOT}/${name}"
    if [[ -L "${dest_dir}" && "$(readlink "${dest_dir}")" == "${source_dir}" ]]; then
      continue
    fi
    if [[ -e "${dest_dir}" || -L "${dest_dir}" ]]; then
      if [[ ! -d "${dest_dir}" ]] \
        || { ! owned_marker "${dest_dir}/${SKILL_MARKER_NAME}" \
          && ! legacy_owned_skill "${source_dir}" "${dest_dir}"; }; then
        printf 'refusing to replace unowned skill path: %s\n' "${dest_dir}" >&2
        return 1
      fi
      if [[ -L "${dest_dir}/SKILL.md" ]]; then
        printf 'refusing to follow skill-file symlink: %s\n' "${dest_dir}/SKILL.md" >&2
        return 1
      fi
    fi
  done

  if [[ "${INSTALL_SYSTEMD}" != "0" ]] && command -v systemctl >/dev/null 2>&1; then
    local service_path="${SYSTEMD_USER_DIR}/${UI_SERVICE_NAME}"
    if [[ -e "${service_path}" || -L "${service_path}" ]] && ! owned_service "${service_path}"; then
      printf 'refusing to replace unowned service: %s\n' "${service_path}" >&2
      return 1
    fi
  fi
}

listener_pid_for_bind() {
  local bind="$1"
  local port="${bind##*:}"
  command -v ss >/dev/null 2>&1 || return 0
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

retire_legacy_ui_service() {
  if [[ "${LEGACY_UI_SERVICE_NAME}" == "${UI_SERVICE_NAME}" ]]; then
    return 0
  fi
  local legacy_service_path="${SYSTEMD_USER_DIR}/${LEGACY_UI_SERVICE_NAME}"
  if [[ -e "${legacy_service_path}" || -L "${legacy_service_path}" ]]; then
    if ! owned_service "${legacy_service_path}"; then
      printf 'preserved unowned legacy service: %s\n' "${legacy_service_path}" >&2
      return 0
    fi
    if command -v systemctl >/dev/null 2>&1; then
      systemctl --user disable --now "${LEGACY_UI_SERVICE_NAME}" >/dev/null 2>&1 || true
    fi
    rm -f "${legacy_service_path}"
  fi
}

install_ui_service() {
  if [[ "${INSTALL_SYSTEMD}" == "0" ]]; then
    printf 'navigator service install disabled by FIDGET_SPINNER_INSTALL_SYSTEMD=0\n'
    return 0
  fi
  if ! command -v systemctl >/dev/null 2>&1; then
    printf 'systemctl unavailable; skipping navigator service install\n' >&2
    return 0
  fi

  local service_path="${SYSTEMD_USER_DIR}/${UI_SERVICE_NAME}"
  local template_path="${SYSTEMD_TEMPLATE_ROOT}/fidget-spinner-ui.service.in"
  local staging_dir
  local staged_service
  mkdir -p "${SYSTEMD_USER_DIR}"
  retire_legacy_ui_service
  if [[ -e "${service_path}" || -L "${service_path}" ]] && ! owned_service "${service_path}"; then
    printf 'refusing to replace unowned service: %s\n' "${service_path}" >&2
    return 1
  fi
  staging_dir="$(mktemp -d "${SYSTEMD_USER_DIR}/.fidget-spinner-unit.XXXXXX")"
  staged_service="${staging_dir}/${UI_SERVICE_NAME}"
  sed \
    -e "s|@HOME@|$(escape_sed_replacement "$(escape_systemd_value "${HOME}")")|g" \
    -e "s|@LOCAL_BIN_DIR@|$(escape_sed_replacement "$(escape_systemd_value "${LOCAL_BIN_DIR}")")|g" \
    -e "s|@UI_BIND@|$(escape_sed_replacement "$(escape_systemd_value "${UI_BIND}")")|g" \
    "${template_path}" > "${staged_service}"
  chmod 0644 "${staged_service}"
  if command -v systemd-analyze >/dev/null 2>&1 \
    && ! systemd-analyze verify "${staged_service}"; then
    rm -f "${staged_service}"
    rmdir "${staging_dir}"
    printf 'refusing invalid generated service: %s\n' "${service_path}" >&2
    return 1
  fi
  mv -f "${staged_service}" "${service_path}"
  rmdir "${staging_dir}"
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

uninstall_local() {
  local service_path="${SYSTEMD_USER_DIR}/${UI_SERVICE_NAME}"
  if [[ -e "${service_path}" || -L "${service_path}" ]]; then
    if owned_service "${service_path}"; then
      if [[ "${INSTALL_SYSTEMD}" != "0" ]] && command -v systemctl >/dev/null 2>&1; then
        systemctl --user disable --now "${UI_SERVICE_NAME}" >/dev/null 2>&1 || true
      fi
      rm -f "${service_path}"
      printf 'removed user service: %s\n' "${service_path}"
    else
      printf 'preserved unowned service: %s\n' "${service_path}" >&2
    fi
  fi
  if [[ "${INSTALL_SYSTEMD}" != "0" ]] && command -v systemctl >/dev/null 2>&1; then
    systemctl --user daemon-reload >/dev/null 2>&1 || true
  fi
  uninstall_binary
  uninstall_skill "fidget-spinner"
  uninstall_skill "frontier-loop"
  printf 'removed Fidget Spinner programs; ledger state under the XDG state root was preserved\n'
}

if [[ "${MODE}" == "uninstall" ]]; then
  uninstall_local
  exit 0
fi

preflight_install

command -v jq >/dev/null || {
  printf 'jq is required to resolve the configured Cargo target directory\n' >&2
  exit 1
}

CARGO_TARGET_DIR="$(
  cargo metadata --format-version 1 --no-deps --manifest-path "${ROOT_DIR}/Cargo.toml" |
    jq -er '.target_directory'
)"

mkdir -p "${LOCAL_BIN_DIR}"

cargo build --release -p fidget-spinner-cli --manifest-path "${ROOT_DIR}/Cargo.toml"
"${CARGO_TARGET_DIR}/release/fidget-spinner-cli" ui serve --bind "${UI_BIND}" --help >/dev/null
install_binary
install_skill "fidget-spinner"
install_skill "frontier-loop"
install_ui_service

printf 'mcp command: %s\n' "${LOCAL_BIN_DIR}/fidget-spinner-cli mcp serve"
