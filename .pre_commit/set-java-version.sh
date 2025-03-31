#!/bin/bash

# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Function to get Java home on Ubuntu
set_java_home_ubuntu() {
  local version="$1"

  echo "Setting Java version to $version on Ubuntu..."
  if [ -d "/usr/lib/jvm/java-${version}-openjdk-amd64" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-${version}-openjdk-amd64"
    export PATH="${JAVA_HOME}:${PATH}"
  elif [ -d "/usr/lib/jvm/java-${version}-openjdk" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-${version}-openjdk"
    export PATH="${JAVA_HOME}:${PATH}"
  else
    echo "Java version ${version} is not installed on your Ubuntu system."
    exit 1
  fi
}

# Function to set Java version on macOS
set_java_home_macos() {
  local version="$1"

  echo "Setting Java version to $version on macOS..."

  # Find the JAVA_HOME path for the specified version
  local java_home_path
  java_home_path=$(/usr/libexec/java_home -v "$version" 2>/dev/null)

  if [[ -z "$java_home_path" ]]; then
    echo "Java version $version is not installed on your macOS system."
    exit 1
  fi

  export JAVA_HOME="$java_home_path";
}

# Main function to set Java version based on OS
set_java_version() {
  local version="$1"
  
  if [[ -n "$JAVA_HOME" && "$JAVA_HOME" =~ $version ]]; then
    echo "JAVA_HOME is already set to a compatible version: $JAVA_HOME"
    return 0
  fi

  if [ -z "$version" ]; then
    echo "No Java version specified. Usage: set_java_version <java_version>"
    echo "Example: set_java_version 11"
    return 1
  fi

  # Detect the operating system
  local os_name
  os_name="$(uname -s)"

  case "$os_name" in
    Linux*)
      set_java_home_ubuntu "$version"
      ;;
    Darwin*)
      set_java_home_macos "$version"
      ;;
    *)
      echo "Unsupported operating system: $os_name"
      return 1
      ;;
  esac

  echo "Java version set to $version successfully!"
  return 0
}
