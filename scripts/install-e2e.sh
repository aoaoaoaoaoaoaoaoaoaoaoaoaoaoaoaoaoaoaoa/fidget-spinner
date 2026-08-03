#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALLER="${ROOT_DIR}/scripts/install-local.sh"
HOST_CARGO_HOME="${CARGO_HOME:-${HOME}/.cargo}"
HOST_CARGO_TARGET_DIR="$(
  cargo metadata --format-version 1 --no-deps --manifest-path "${ROOT_DIR}/Cargo.toml" |
    jq -er '.target_directory'
)"
SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/fidget-spinner-install-e2e.XXXXXX")"
HOME_ROOT="${SANDBOX}/home with %"
LOCAL_ROOT="${HOME_ROOT}/local root"
SKILL_ROOT="${HOME_ROOT}/codex skills"
XDG_CONFIG_ROOT="${SANDBOX}/xdg config %"
XDG_STATE_ROOT="${SANDBOX}/xdg state"
FAKE_BIN="${SANDBOX}/fake-bin"
SYSTEMCTL_LOG="${SANDBOX}/systemctl.log"
SERVICE_PATH="${XDG_CONFIG_ROOT}/systemd/user/fidget-spinner-ui.service"

cleanup() {
  rm -rf -- "${SANDBOX}"
}
trap cleanup EXIT

fail() {
  printf 'install E2E failure: %s\n' "$1" >&2
  exit 1
}

run_installer() {
  env \
    HOME="${HOME_ROOT}" \
    PATH="${FAKE_BIN}:${PATH}" \
    XDG_CONFIG_HOME="${XDG_CONFIG_ROOT}" \
    XDG_STATE_HOME="${XDG_STATE_ROOT}" \
    XDG_RUNTIME_DIR="${SANDBOX}/runtime" \
    DBUS_SESSION_BUS_ADDRESS= \
    SYSTEMCTL_LOG="${SYSTEMCTL_LOG}" \
    FIDGET_SPINNER_UI_BIND="${TEST_UI_BIND:-127.0.0.1:0}" \
    CARGO_HOME="${HOST_CARGO_HOME}" \
    CARGO_TARGET_DIR="${HOST_CARGO_TARGET_DIR}" \
    "${INSTALLER}" "$@"
}

mkdir -p "${FAKE_BIN}" "${HOME_ROOT}" "${XDG_CONFIG_ROOT}/systemd/user" "${SANDBOX}/runtime"
cat > "${FAKE_BIN}/systemctl" <<'SYSTEMCTL'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${SYSTEMCTL_LOG}"
if [[ "$*" == *" is-enabled "* ]]; then
  exit 1
fi
SYSTEMCTL
chmod 0755 "${FAKE_BIN}/systemctl"

if TEST_UI_BIND=0.0.0.0:8913 run_installer "${LOCAL_ROOT}" "${SKILL_ROOT}" >/dev/null 2>&1; then
  fail "service installer accepted a non-loopback bind"
fi

printf 'foreign unit\n' > "${SERVICE_PATH}"
if run_installer "${LOCAL_ROOT}" "${SKILL_ROOT}" >/dev/null 2>&1; then
  fail "installer replaced an unowned service"
fi
[[ "$(<"${SERVICE_PATH}")" == "foreign unit" ]] || fail "unowned service bytes changed"
[[ ! -e "${LOCAL_ROOT}/bin/fidget-spinner-cli" ]] || fail "preflight failure left a binary"
[[ ! -e "${SKILL_ROOT}/fidget-spinner" ]] || fail "preflight failure left a skill"
rm -f "${SERVICE_PATH}"

mkdir -p "${XDG_STATE_ROOT}/fidget-spinner/projects/sentinel"
printf 'ledger\n' > "${XDG_STATE_ROOT}/fidget-spinner/projects/sentinel/state.sqlite"
run_installer "${LOCAL_ROOT}" "${SKILL_ROOT}"

"${LOCAL_ROOT}/bin/fidget-spinner-cli" --version | grep -Eq '^fidget-spinner-cli 1\.0\.0$' \
  || fail "installed binary has the wrong identity"
for skill in fidget-spinner frontier-loop; do
  [[ -f "${SKILL_ROOT}/${skill}/SKILL.md" ]] || fail "${skill} was not copied"
  [[ ! -L "${SKILL_ROOT}/${skill}" ]] || fail "${skill} still depends on the source checkout"
  [[ -f "${SKILL_ROOT}/${skill}/.fidget-spinner-owned" ]] || fail "${skill} lacks ownership proof"
done
[[ -f "${SERVICE_PATH}" ]] || fail "service ignored absolute XDG_CONFIG_HOME"
head -n 1 "${SERVICE_PATH}" | grep -Fqx '# managed by fidget-spinner installer' \
  || fail "service lacks ownership proof"
grep -Fq '%%' "${SERVICE_PATH}" || fail "systemd percent specifiers were not escaped"
systemd-analyze verify "${SERVICE_PATH}" || fail "generated service fails the real unit parser"

printf 'preserve me\n' > "${SKILL_ROOT}/fidget-spinner/user-note"
run_installer --uninstall "${LOCAL_ROOT}" "${SKILL_ROOT}"

[[ ! -e "${LOCAL_ROOT}/bin/fidget-spinner-cli" ]] || fail "binary survived uninstall"
[[ ! -e "${SERVICE_PATH}" ]] || fail "owned service survived uninstall"
[[ -f "${SKILL_ROOT}/fidget-spinner/user-note" ]] || fail "uninstall removed user material"
[[ ! -e "${SKILL_ROOT}/fidget-spinner/SKILL.md" ]] || fail "owned skill file survived uninstall"
[[ -f "${XDG_STATE_ROOT}/fidget-spinner/projects/sentinel/state.sqlite" ]] \
  || fail "uninstall removed ledger state"

printf 'foreign unit after uninstall\n' > "${SERVICE_PATH}"
run_installer --uninstall "${LOCAL_ROOT}" "${SKILL_ROOT}"
[[ "$(<"${SERVICE_PATH}")" == "foreign unit after uninstall" ]] \
  || fail "uninstall removed an unowned service"

printf 'install E2E passed\n'
