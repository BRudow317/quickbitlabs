#!/bin/bash
# search_in_files - Search for a pattern in files within a directory
search_in_files() {
    local pattern="$1"
    local directory="${2:-.}"
    local file_pattern="${3:-*}"
    
    if [[ -z "$pattern" ]]; then
        echo "Error: Search pattern required" >&2
        return 1
    fi
    
    if [[ ! -d "$directory" ]]; then
        echo "Error: Directory '$directory' does not exist" >&2
        return 1
    fi
    
    echo "Searching for '$pattern' in $directory (files: $file_pattern)"
    echo "----------------------------------------"
    grep -rn --include="$file_pattern" --color=auto "$pattern" "$directory" 2>/dev/null
    local count=$(grep -rl --include="$file_pattern" "$pattern" "$directory" 2>/dev/null | wc -l)
    echo "----------------------------------------"
    echo "Found in $count file(s)"
}

# ----------------------------------------------------------------------------
# find_large_files - Find files larger than specified size
find_large_files() {
    local directory="${1:-.}"
    local size="${2:-100M}"
    
    if [[ ! -d "$directory" ]]; then
        echo "Error: Directory '$directory' does not exist" >&2
        return 1
    fi
    
    echo "Files larger than $size in $directory:"
    echo "----------------------------------------"
    find "$directory" -type f -size +$size -exec ls -lh {} \; 2>/dev/null | \
        awk '{print $5, $9}' | sort -hr
}

# ----------------------------------------------------------------------------
# find_recent_files - Find files modified within N days
find_recent_files() {
    local directory="${1:-.}"
    local days="${2:-1}"
    
    if [[ ! -d "$directory" ]]; then
        echo "Error: Directory '$directory' does not exist" >&2
        return 1
    fi
    
    echo "Files modified in last $days day(s) in $directory:"
    echo "----------------------------------------"
    find "$directory" -type f -mtime -$days -exec ls -lh {} \; 2>/dev/null | \
        awk '{print $6, $7, $8, $9}'
}

# ----------------------------------------------------------------------------
# count_lines - Count lines in files matching a pattern
count_lines() {
    local directory="${1:-.}"
    local file_pattern="${2:-*}"
    
    if [[ ! -d "$directory" ]]; then
        echo "Error: Directory '$directory' does not exist" >&2
        return 1
    fi
    
    echo "Line counts for '$file_pattern' files in $directory:"
    echo "----------------------------------------"
    find "$directory" -type f -name "$file_pattern" -exec wc -l {} \; 2>/dev/null | \
        sort -n | tail -20
    echo "----------------------------------------"
    local total=$(find "$directory" -type f -name "$file_pattern" -exec cat {} \; 2>/dev/null | wc -l)
    echo "Total lines: $total"
}

# ----------------------------------------------------------------------------
# replace_in_files - Find and replace text in files (with backup)
replace_in_files() {
    local find_text="$1"
    local replace_text="$2"
    local file_pattern="$3"
    local directory="${4:-.}"
    
    if [[ -z "$find_text" || -z "$file_pattern" ]]; then
        echo "Error: find_text and file_pattern required" >&2
        return 1
    fi
    
    echo "Replacing '$find_text' with '$replace_text' in $file_pattern files"
    
    local files=$(grep -rl --include="$file_pattern" "$find_text" "$directory" 2>/dev/null)
    
    if [[ -z "$files" ]]; then
        echo "No files found containing '$find_text'"
        return 0
    fi
    
    local count=0
    for file in $files; do
        cp "$file" "$file.bak"
        sed -i "s|$find_text|$replace_text|g" "$file"
        echo "  Updated: $file (backup: $file.bak)"
        ((count++))
    done
    
    echo "Updated $count file(s)"
}

# ----------------------------------------------------------------------------
# extract_between - Extract lines between two patterns
extract_between() {
    local start_pattern="$1"
    local end_pattern="$2"
    local file="$3"
    
    if [[ -z "$start_pattern" || -z "$end_pattern" || -z "$file" ]]; then
        echo "Error: start_pattern, end_pattern, and file required" >&2
        return 1
    fi
    
    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' does not exist" >&2
        return 1
    fi
    
    sed -n "/$start_pattern/,/$end_pattern/p" "$file"
}

# ----------------------------------------------------------------------------
# tail_grep - Tail a file and grep for pattern (live monitoring)
tail_grep() {
    local pattern="$1"
    local file="$2"
    local num_lines="${3:-0}"
    
    if [[ -z "$pattern" || -z "$file" ]]; then
        echo "Error: pattern and file required" >&2
        return 1
    fi
    
    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' does not exist" >&2
        return 1
    fi
    
    echo "Monitoring '$file' for pattern '$pattern' (Ctrl+C to stop)"
    if [[ "$num_lines" -gt 0 ]]; then
        tail -n "$num_lines" -f "$file" | grep --line-buffered --color=auto "$pattern"
    else
        tail -f "$file" | grep --line-buffered --color=auto "$pattern"
    fi
}

# ----------------------------------------------------------------------------
# backup_file - Create a timestamped backup of a file
backup_file() {
    local file="$1"
    local backup_dir="${2:-$(dirname "$file")}"
    
    if [[ -z "$file" ]]; then
        echo "Error: File path required" >&2
        return 1
    fi
    
    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' does not exist" >&2
        return 1
    fi
    
    mkdir -p "$backup_dir"
    
    local filename=$(basename "$file")
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_path="$backup_dir/${filename}.${timestamp}.bak"
    
    if cp "$file" "$backup_path"; then
        echo "Backup created: $backup_path"
        return 0
    else
        echo "Error: Failed to create backup" >&2
        return 1
    fi
}

# ----------------------------------------------------------------------------
# find_duplicates - Find duplicate files by content (MD5)
find_duplicates() {
    local directory="${1:-.}"
    
    if [[ ! -d "$directory" ]]; then
        echo "Error: Directory '$directory' does not exist" >&2
        return 1
    fi
    
    echo "Finding duplicate files in $directory..."
    echo "----------------------------------------"
    
    find "$directory" -type f -exec md5sum {} \; 2>/dev/null | \
        sort | \
        uniq -w32 -d --all-repeated=separate | \
        awk '{print $2}'
}

# ----------------------------------------------------------------------------
# grep_context - Search with context lines before and after
grep_context() {
    local pattern="$1"
    local file="$2"
    local context="${3:-3}"
    
    if [[ -z "$pattern" || -z "$file" ]]; then
        echo "Error: pattern and file required" >&2
        return 1
    fi
    
    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' does not exist" >&2
        return 1
    fi
    
    grep -n -C "$context" --color=auto "$pattern" "$file"
}