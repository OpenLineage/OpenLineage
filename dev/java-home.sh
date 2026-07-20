#!/bin/sh
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Resolve JAVA_HOME for the requested major Java version. Used by the
# component Taskfiles to pin the JDK per component instead of relying on
# whatever happens to be active in the developer's shell.
#
# Resolution order:
#   1. The developer's own shell: $SHELL is started as a login interactive
#      shell (so profile hooks like SDKMAN!/jenv apply) and its java is used
#      IF it already matches the requested major version.
#   2. SDKMAN! installs (~/.sdkman/candidates/java).
#   3. macOS /usr/libexec/java_home.
#   4. The inherited JAVA_HOME (what CI provides via setup-java / CI image).
#
# Version selection: the JAVA_VERSION environment variable (explicit
# override, e.g. `JAVA_VERSION=21 task build`), else the first argument
# (the component's pinned version), else 17. Java 11 is the minimum —
# the legacy 1.x version scheme (Java 8 and older) is not supported.

v="${JAVA_VERSION:-${1:-17}}"
case "$v" in
  *[!0-9]* | '')
    echo "java-home.sh: '$v' is not a valid major Java version (integer >= 11)" >&2
    exit 1
    ;;
esac
if [ "$v" -lt 11 ]; then
  echo "java-home.sh: Java $v is not supported, the minimum is 11" >&2
  exit 1
fi

# Print the major version of the java installation at $1, e.g. 17 for
# "17.0.12". Java 8-style "1.8.0_392" parses as major 1 and never matches.
java_major() {
  ver=$("$1/bin/java" -version 2>&1 | awk -F'"' '/version/ {print $2; exit}')
  echo "${ver%%[.+]*}"
}

# 1) The developer's shell, profile hooks applied. Skipped in CI (no user
#    profile worth consulting). Starting a login interactive shell takes
#    seconds, so the probed value (which is version-independent) is cached
#    for 24h. The output is marker-prefixed so banners/noise from the
#    interactive shell cannot pollute the result.
if [ -z "${CI:-}" ] && [ -n "${SHELL:-}" ] && [ -x "$SHELL" ]; then
  cache="${TMPDIR:-/tmp}/openlineage-java-home-shell-$(id -u)"
  if [ -f "$cache" ] && [ -n "$(find "$cache" -mmin -1440 2>/dev/null)" ]; then
    shell_home=$(cat "$cache")
  else
    # The single-quoted script is intentional: these expressions must expand
    # in the spawned login-interactive $SHELL (with its profile hooks), not here.
    # shellcheck disable=SC2016
    shell_home=$("$SHELL" -l -i -c '
      if [ -n "${JAVA_HOME:-}" ]; then
        printf "OL_JAVA_HOME:%s\n" "$JAVA_HOME"
      elif command -v java >/dev/null 2>&1; then
        printf "OL_JAVA_HOME:%s\n" "$(cd "$(dirname "$(command -v java)")/.." && pwd -P)"
      fi' 2>/dev/null | sed -n 's/^OL_JAVA_HOME://p' | tail -n 1)
    printf '%s\n' "$shell_home" > "$cache"
  fi
  if [ -n "$shell_home" ] && [ -x "$shell_home/bin/java" ]; then
    if [ "$(java_major "$shell_home")" = "$v" ]; then
      echo "$shell_home"
      exit 0
    fi
  fi
fi

# 2) SDKMAN! installs.
for d in "$HOME/.sdkman/candidates/java/$v."*; do
  if [ -d "$d" ]; then echo "$d"; exit 0; fi
done

# 3) macOS java_home, then 4) inherited JAVA_HOME.
/usr/libexec/java_home -v "$v" 2>/dev/null || echo "${JAVA_HOME:-}"
