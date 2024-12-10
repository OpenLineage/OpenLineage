#!/bin/bash

# Function to set Java version on Ubuntu
set_java_version_ubuntu() {
  local version="$1"

  echo "Setting Java version to $version on Ubuntu..."
  if [ -d "/usr/lib/jvm/java-${version}-openjdk-amd64" ]; then
    sudo update-alternatives --set java "/usr/lib/jvm/java-${version}-openjdk-amd64/bin/java"
    sudo update-alternatives --set javac "/usr/lib/jvm/java-${version}-openjdk-amd64/bin/javac"
  else
    echo "Java version ${version} is not installed on your Ubuntu system."
    exit 1
  fi
}

# Function to set Java version on macOS
set_java_version_macos() {
  local version="$1"

  echo "Setting Java version to $version on macOS..."

  # Find the JAVA_HOME path for the specified version
  local java_home_path
  java_home_path=$(/usr/libexec/java_home -v "$version" 2>/dev/null)

  if [[ -z "$java_home_path" ]]; then
    echo "Java version $version is not installed on your macOS system."
    exit 1
  fi

  export JAVA_HOME="$java_home_path"
  echo "JAVA_HOME set to $JAVA_HOME"
  # Append the JAVA_HOME export to the user's profile if not already set
  if ! grep -q "export JAVA_HOME=$JAVA_HOME" ~/.bash_profile; then
    echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bash_profile
  fi

  # Apply the changes to the current shell session
  source ~/.bash_profile
}

# Main function to set Java version based on OS
set_java_version() {
  local version="$1"

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
      set_java_version_ubuntu "$version"
      ;;
    Darwin*)
      set_java_version_macos "$version"
      ;;
    *)
      echo "Unsupported operating system: $os_name"
      return 1
      ;;
  esac

  echo "Java version set to $version successfully!"
  return 0
}
