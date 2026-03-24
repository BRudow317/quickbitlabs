#!/bin/bash

# install_pkg - Install package with automatic package manager detection for any distro
install_pkg() {
    if [[ $# -eq 0 ]]; then
        echo "Error: At least one package name required" >&2
        return 1
    fi
    
    local packages="$@"
    
    if command -v apt &>/dev/null; then
        echo "Using apt to install: $packages"
        sudo apt update && sudo apt install -y $packages
    elif command -v dnf &>/dev/null; then
        echo "Using dnf to install: $packages"
        sudo dnf install -y $packages
    elif command -v yum &>/dev/null; then
        echo "Using yum to install: $packages"
        sudo yum install -y $packages
    elif command -v pacman &>/dev/null; then
        echo "Using pacman to install: $packages"
        sudo pacman -S --noconfirm $packages
    elif command -v zypper &>/dev/null; then
        echo "Using zypper to install: $packages"
        sudo zypper install -y $packages
    else
        echo "Error: No supported package manager found" >&2
        return 1
    fi
}


# remove_pkg - Remove package with automatic package manager detection
remove_pkg() {
    if [[ $# -eq 0 ]]; then
        echo "Error: At least one package name required" >&2
        return 1
    fi
    
    local packages="$@"
    
    if command -v apt &>/dev/null; then
        echo "Using apt to remove: $packages"
        sudo apt remove -y $packages
    elif command -v dnf &>/dev/null; then
        echo "Using dnf to remove: $packages"
        sudo dnf remove -y $packages
    elif command -v yum &>/dev/null; then
        echo "Using yum to remove: $packages"
        sudo yum remove -y $packages
    elif command -v pacman &>/dev/null; then
        echo "Using pacman to remove: $packages"
        sudo pacman -R --noconfirm $packages
    elif command -v zypper &>/dev/null; then
        echo "Using zypper to remove: $packages"
        sudo zypper remove -y $packages
    else
        echo "Error: No supported package manager found" >&2
        return 1
    fi
}


# search_pkg - Search for packages in repositories
search_pkg() {
    local search_term="$1"
    
    if [[ -z "$search_term" ]]; then
        echo "Error: Search term required" >&2
        return 1
    fi
    
    if command -v apt &>/dev/null; then
        echo "Searching apt for: $search_term"
        apt search "$search_term" 2>/dev/null
    elif command -v dnf &>/dev/null; then
        echo "Searching dnf for: $search_term"
        dnf search "$search_term"
    elif command -v yum &>/dev/null; then
        echo "Searching yum for: $search_term"
        yum search "$search_term"
    elif command -v pacman &>/dev/null; then
        echo "Searching pacman for: $search_term"
        pacman -Ss "$search_term"
    elif command -v zypper &>/dev/null; then
        echo "Searching zypper for: $search_term"
        zypper search "$search_term"
    else
        echo "Error: No supported package manager found" >&2
        return 1
    fi
}


# list_installed - List installed packages (optionally filtered)
list_installed() {
    local filter="$1"
    
    if command -v apt &>/dev/null; then
        if [[ -n "$filter" ]]; then
            dpkg -l | grep -i "$filter"
        else
            dpkg -l | tail -n +6
        fi
    elif command -v dnf &>/dev/null; then
        if [[ -n "$filter" ]]; then
            dnf list installed | grep -i "$filter"
        else
            dnf list installed
        fi
    elif command -v rpm &>/dev/null; then
        if [[ -n "$filter" ]]; then
            rpm -qa | grep -i "$filter"
        else
            rpm -qa
        fi
    elif command -v pacman &>/dev/null; then
        if [[ -n "$filter" ]]; then
            pacman -Q | grep -i "$filter"
        else
            pacman -Q
        fi
    else
        echo "Error: No supported package manager found" >&2
        return 1
    fi
}


# update_system - Update all system packages
update_system() {
    echo "Updating system packages..."
    
    if command -v apt &>/dev/null; then
        echo "Using apt..."
        sudo apt update && sudo apt upgrade -y && sudo apt autoremove -y
    elif command -v dnf &>/dev/null; then
        echo "Using dnf..."
        sudo dnf upgrade -y && sudo dnf autoremove -y
    elif command -v yum &>/dev/null; then
        echo "Using yum..."
        sudo yum update -y
    elif command -v pacman &>/dev/null; then
        echo "Using pacman..."
        sudo pacman -Syu --noconfirm
    elif command -v zypper &>/dev/null; then
        echo "Using zypper..."
        sudo zypper update -y
    else
        echo "Error: No supported package manager found" >&2
        return 1
    fi
    
    echo "System update complete!"
}