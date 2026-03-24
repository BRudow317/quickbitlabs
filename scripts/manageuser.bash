#!/bin/bash

# ----------------------------------------------------------------------------
# 1. create_user - Create a new user with optional home directory and shell
# Usage: create_user <username> [shell] [create_home: yes/no]
# Example: create_user john /bin/bash yes

create_user() {
    local username="$1"
    local shell="${2:-/bin/bash}"
    local create_home="${3:-yes}"
    
    if [[ -z "$username" ]]; then
        echo "Error: Username required" >&2
        return 1
    fi
    
    if id "$username" &>/dev/null; then
        echo "Error: User '$username' already exists" >&2
        return 1
    fi
    
    local opts="-s $shell"
    [[ "$create_home" == "yes" ]] && opts="$opts -m"
    
    if sudo useradd $opts "$username"; then
        echo "User '$username' created successfully"
        echo "Set password with: sudo passwd $username"
        return 0
    else
        echo "Error: Failed to create user '$username'" >&2
        return 1
    fi
}

# ----------------------------------------------------------------------------
# 2. delete_user - Remove a user and optionally their home directory
# Usage: delete_user <username> [remove_home: yes/no]
# Example: delete_user john yes

delete_user() {
    local username="$1"
    local remove_home="${2:-no}"
    
    if [[ -z "$username" ]]; then
        echo "Error: Username required" >&2
        return 1
    fi
    
    if ! id "$username" &>/dev/null; then
        echo "Error: User '$username' does not exist" >&2
        return 1
    fi
    
    local opts=""
    [[ "$remove_home" == "yes" ]] && opts="-r"
    
    if sudo userdel $opts "$username"; then
        echo "User '$username' deleted successfully"
        return 0
    else
        echo "Error: Failed to delete user '$username'" >&2
        return 1
    fi
}

# ----------------------------------------------------------------------------
# 3. add_user_to_group - Add an existing user to a group
# Usage: add_user_to_group <username> <groupname>
# Example: add_user_to_group john docker

add_user_to_group() {
    local username="$1"
    local groupname="$2"
    
    if [[ -z "$username" || -z "$groupname" ]]; then
        echo "Error: Username and groupname required" >&2
        return 1
    fi
    
    if ! id "$username" &>/dev/null; then
        echo "Error: User '$username' does not exist" >&2
        return 1
    fi
    
    if ! getent group "$groupname" &>/dev/null; then
        echo "Error: Group '$groupname' does not exist" >&2
        return 1
    fi
    
    if sudo usermod -aG "$groupname" "$username"; then
        echo "User '$username' added to group '$groupname'"
        return 0
    else
        echo "Error: Failed to add user to group" >&2
        return 1
    fi
}

# ----------------------------------------------------------------------------
# 4. list_user_groups - Show all groups a user belongs to
# Usage: list_user_groups <username>
# Example: list_user_groups john

list_user_groups() {
    local username="${1:-$(whoami)}"
    
    if ! id "$username" &>/dev/null; then
        echo "Error: User '$username' does not exist" >&2
        return 1
    fi
    
    echo "Groups for user '$username':"
    groups "$username" | cut -d: -f2 | tr ' ' '\n' | grep -v '^$' | sort | sed 's/^/  - /'
}

# ----------------------------------------------------------------------------
# 5. create_group - Create a new group with optional GID
# Usage: create_group <groupname> [gid]
# Example: create_group developers 1500

create_group() {
    local groupname="$1"
    local gid="$2"
    
    if [[ -z "$groupname" ]]; then
        echo "Error: Group name required" >&2
        return 1
    fi
    
    if getent group "$groupname" &>/dev/null; then
        echo "Error: Group '$groupname' already exists" >&2
        return 1
    fi
    
    local opts=""
    [[ -n "$gid" ]] && opts="-g $gid"
    
    if sudo groupadd $opts "$groupname"; then
        echo "Group '$groupname' created successfully"
        return 0
    else
        echo "Error: Failed to create group '$groupname'" >&2
        return 1
    fi
}