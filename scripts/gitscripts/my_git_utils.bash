#!/bin/bash

# Comprehensive repository status
# git_status_full

git_status_full() {
    echo "=== Repository Status ==="
    echo ""
    echo "Branch: $(git branch --show-current)"
    echo "Remote: $(git remote get-url origin 2>/dev/null || echo 'No remote')"
    echo ""
    
    # Tracking status
    local tracking=$(git rev-parse --abbrev-ref --symbolic-full-name @{u} 2>/dev/null)
    if [[ -n "$tracking" ]]; then
        local ahead=$(git rev-list --count "$tracking"..HEAD 2>/dev/null)
        local behind=$(git rev-list --count HEAD.."$tracking" 2>/dev/null)
        echo "Tracking: $tracking ($ahead ahead, $behind behind)"
    fi
    echo ""
    
    echo "=== Working Directory ==="
    git status -s
    echo ""
    
    echo "=== Recent Commits ==="
    git log --oneline -5
    echo ""
    
    echo "=== Stashes ==="
    git stash list | head -5 || echo "No stashes"
}


# Pretty formatted git log
# git_log_pretty [num_commits]
# Ex: git_log_pretty 20

git_log_pretty() {
    local count="${1:-15}"
    
    git log \
        --graph \
        --pretty=format:'%Cred%h%Creset -%C(yellow)%d%Creset %s %Cgreen(%cr) %C(bold blue)<%an>%Creset' \
        --abbrev-commit \
        -"$count"
}


# git_aliases - Show all git aliases
# git_aliases

git_aliases() {
    echo "=== Git Aliases ==="
    git config --get-regexp alias | sed 's/alias\.//' | while read alias command; do
        printf "  %-15s = %s\n" "$alias" "$command"
    done
}


# 40. git_undo - Undo last commit (keep changes)
#  git_undo [num_commits]
# Example: git_undo 2

git_undo() {
    local count="${1:-1}"
    
    echo "Commits to undo:"
    git log --oneline -"$count"
    echo ""
    
    read -p "Undo $count commit(s)? Changes will be kept. [Y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy] ]]; then
        git reset --soft HEAD~"$count"
        echo " Undone. Changes are staged."
    fi
}

# easy staging manager
# git_stage_interactive

git_stage_interactive() {
    echo "=== Unstaged Changes ==="
    git status -s | grep -E '^ [MD\?]' || echo "No unstaged changes"
    
    echo ""
    echo "Options:"
    echo "  1. Stage all changes"
    echo "  2. Stage specific files (interactive)"
    echo "  3. Stage by hunks (patch mode)"
    echo "  4. View diff first"
    echo "  5. Cancel"
    echo ""
    read -p "Choose [1-5]: " choice
    
    case "$choice" in
        1)
            git add -A
            echo "All changes staged"
            ;;
        2)
            git add -i
            ;;
        3)
            git add -p
            ;;
        4)
            git diff
            echo ""
            read -p "Stage all these changes? [Y/N]: " confirm
            [[ "$confirm" =~ ^[Yy] ]] && git add -A
            ;;
        5)
            echo "Cancelled"
            return 0
            ;;
        *)
            echo "Invalid choice"
            return 1
            ;;
    esac
    
    echo ""
    echo "=== Staged Changes ==="
    git status -s | grep -E '^[MADRC]'
}


# 8. git_stage_by_type - Stage files by extension/type
#  git_stage_by_type <extension>
# Example: git_stage_by_type js

git_stage_by_type() {
    local extension="$1"
    
    if [[ -z "$extension" ]]; then
        echo "Error: File extension required" >&2
        echo "Usage: git_stage_by_type <extension>" >&2
        return 1
    fi
    
    # Remove leading dot if provided
    extension="${extension#.}"
    
    local files=$(git ls-files --modified --others --exclude-standard | grep "\.${extension}$")
    
    if [[ -z "$files" ]]; then
        echo "No modified *.$extension files found"
        return 0
    fi
    
    echo "Files to stage:"
    echo "$files" | sed 's/^/  /'
    echo ""
    
    read -p "Stage these files? [Y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy] ]]; then
        echo "$files" | xargs git add
        echo " Staged $(echo "$files" | wc -l) file(s)"
    fi
}


# 9. git_unstage - Unstage files (keep changes in working directory)
#  git_unstage [file...] (no args = unstage all)
# Example: git_unstage src/app.js

git_unstage() {
    if [[ $# -eq 0 ]]; then
        echo "Unstaging all files..."
        git reset HEAD
    else
        git reset HEAD -- "$@"
    fi
    
    echo ""
    echo "Current status:"
    git status -s
}


# 10. git_discard - Discard changes in working directory (dangerous!)
#  git_discard [file...] (no args = discard all)
# Example: git_discard src/app.js

git_discard() {
    echo "⚠️  WARNING: This will permanently discard changes!"
    
    if [[ $# -eq 0 ]]; then
        echo "Files to discard:"
        git status -s | grep -E '^ [MD]' | sed 's/^/  /'
        echo ""
        read -p "Discard ALL changes? Type 'yes' to confirm: " confirm
        if [[ "$confirm" == "yes" ]]; then
            git checkout -- .
            git clean -fd
            echo " All changes discarded"
        else
            echo "Cancelled"
        fi
    else
        echo "Discarding: $@"
        read -p "Confirm? [Y/N]: " confirm
        if [[ "$confirm" =~ ^[Yy] ]]; then
            git checkout -- "$@"
            echo " Changes discarded"
        fi
    fi
}


# ============================================================================
# GIT COMMITTING WORKFLOWS
# ============================================================================


# 11. git_commit_conventional - Commit with conventional commit format
#  git_commit_conventional
# Example: git_commit_conventional

git_commit_conventional() {
    # Check for staged changes
    if git diff --cached --quiet; then
        echo "No staged changes to commit"
        echo "Stage changes first with: git add <files>"
        return 1
    fi
    
    echo "Staged changes:"
    git diff --cached --stat
    echo ""
    
    echo "Commit type:"
    echo "  1. feat     - New feature"
    echo "  2. fix      - Bug fix"
    echo "  3. docs     - Documentation"
    echo "  4. style    - Formatting, no code change"
    echo "  5. refactor - Code restructuring"
    echo "  6. test     - Adding tests"
    echo "  7. chore    - Maintenance"
    echo "  8. perf     - Performance improvement"
    echo ""
    read -p "Choose type [1-8]: " type_choice
    
    local commit_type
    case "$type_choice" in
        1) commit_type="feat" ;;
        2) commit_type="fix" ;;
        3) commit_type="docs" ;;
        4) commit_type="style" ;;
        5) commit_type="refactor" ;;
        6) commit_type="test" ;;
        7) commit_type="chore" ;;
        8) commit_type="perf" ;;
        *) echo "Invalid choice"; return 1 ;;
    esac
    
    read -p "Scope (optional, e.g., api, ui): " scope
    read -p "Short description: " description
    
    local commit_msg
    if [[ -n "$scope" ]]; then
        commit_msg="${commit_type}(${scope}): ${description}"
    else
        commit_msg="${commit_type}: ${description}"
    fi
    
    echo ""
    read -p "Add longer body? [Y/N]: " add_body
    if [[ "$add_body" =~ ^[Yy] ]]; then
        echo "Enter body (Ctrl+D when done):"
        local body=$(cat)
        git commit -m "$commit_msg" -m "$body"
    else
        git commit -m "$commit_msg"
    fi
    
    echo ""
    echo " Committed: $commit_msg"
}


# 12. git_commit_quick - Quick commit with message
#  git_commit_quick <message>
# Example: git_commit_quick "Fix login bug"

git_commit_quick() {
    local message="$*"
    
    if [[ -z "$message" ]]; then
        echo "Error: Commit message required" >&2
        return 1
    fi
    
    if git diff --cached --quiet; then
        echo "No staged changes. Staging all modified files..."
        git add -u
    fi
    
    git commit -m "$message"
    echo " Committed: $message"
}


# 13. git_commit_amend - Amend last commit (message or content)
#  git_commit_amend [new_message]
# Example: git_commit_amend "Better commit message"

git_commit_amend() {
    local new_message="$*"
    
    echo "Last commit:"
    git log -1 --oneline
    echo ""
    
    if [[ -n "$new_message" ]]; then
        git commit --amend -m "$new_message"
        echo " Commit message updated"
    else
        if ! git diff --cached --quiet; then
            echo "Staged changes will be added to last commit"
        fi
        git commit --amend
    fi
    
    echo ""
    echo "⚠️  If already pushed, you'll need: git push --force-with-lease"
}


# 14. git_commit_fixup - Create fixup commit for interactive rebase
#  git_commit_fixup <commit_hash>
# Example: git_commit_fixup abc123

git_commit_fixup() {
    local target_commit="$1"
    
    if [[ -z "$target_commit" ]]; then
        echo "Recent commits:"
        git log --oneline -10
        echo ""
        read -p "Enter commit hash to fixup: " target_commit
    fi
    
    if git diff --cached --quiet; then
        echo "Error: No staged changes" >&2
        return 1
    fi
    
    git commit --fixup="$target_commit"
    echo ""
    echo " Fixup commit created"
    echo "To apply: git rebase -i --autosquash ${target_commit}^"
}


# 15. git_wip - Quick work-in-progress commit
#  git_wip [message]
# Example: git_wip "halfway through refactor"

git_wip() {
    local message="${*:-work in progress}"
    
    git add -A
    git commit -m "WIP: $message"
    
    echo " WIP commit created"
    echo "Later, squash with: git reset --soft HEAD~1"
}


# ============================================================================
# GIT PUSHING WORKFLOWS
# ============================================================================


# 16. git_push_safe - Push with checks
#  git_push_safe [remote] [branch]
# Example: git_push_safe origin feature-branch

git_push_safe() {
    local remote="${1:-origin}"
    local branch="${2:-$(git branch --show-current)}"
    
    echo "Pre-push checks..."
    
    # Check if branch exists on remote
    if git ls-remote --exit-code "$remote" "$branch" &>/dev/null; then
        echo "   Remote branch exists"
        
        # Check if we're ahead
        git fetch "$remote" "$branch" --quiet
        local ahead=$(git rev-list --count "$remote/$branch"..HEAD 2>/dev/null)
        local behind=$(git rev-list --count HEAD.."$remote/$branch" 2>/dev/null)
        
        echo "  Status: $ahead ahead, $behind behind"
        
        if [[ "$behind" -gt 0 ]]; then
            echo ""
            echo "⚠️  Remote has commits you don't have!"
            read -p "Pull first? [Y/N]: " pull_first
            if [[ ! "$pull_first" =~ ^[Nn] ]]; then
                git pull --rebase "$remote" "$branch"
            fi
        fi
    else
        echo "  → New branch (will create on remote)"
    fi
    
    echo ""
    echo "Commits to push:"
    git log "$remote/$branch"..HEAD --oneline 2>/dev/null || git log --oneline -5
    echo ""
    
    read -p "Push to $remote/$branch? [Y/N]: " confirm
    if [[ ! "$confirm" =~ ^[Nn] ]]; then
        if git push -u "$remote" "$branch"; then
            echo " Pushed successfully"
        else
            echo "✗ Push failed"
            return 1
        fi
    fi
}


# 17. git_push_force_safe - Force push with lease (safer than --force)
#  git_push_force_safe [remote] [branch]
# Example: git_push_force_safe origin feature-branch

git_push_force_safe() {
    local remote="${1:-origin}"
    local branch="${2:-$(git branch --show-current)}"
    
    echo "⚠️  FORCE PUSH (with lease) to $remote/$branch"
    echo ""
    echo "This will overwrite remote history!"
    echo "Using --force-with-lease for safety (fails if remote changed)"
    echo ""
    
    read -p "Are you sure? Type 'force' to confirm: " confirm
    if [[ "$confirm" == "force" ]]; then
        git push --force-with-lease "$remote" "$branch"
        echo " Force pushed"
    else
        echo "Cancelled"
    fi
}


# 18. git_push_tags - Push all tags to remote
#  git_push_tags [remote]
# Example: git_push_tags origin

git_push_tags() {
    local remote="${1:-origin}"
    
    echo "Local tags:"
    git tag -l | head -20
    echo ""
    
    read -p "Push all tags to $remote? [Y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy] ]]; then
        git push "$remote" --tags
        echo " Tags pushed"
    fi
}


# ============================================================================
# GIT BRANCH HELPER FUNCTIONS
# ============================================================================


# 19. git_branches - Show branches with extra info
#  git_branches [filter]
# Example: git_branches feature

git_branches() {
    local filter="$1"
    
    echo "=== Local Branches ==="
    if [[ -n "$filter" ]]; then
        git branch -vv | grep -i "$filter"
    else
        git branch -vv
    fi
    
    echo ""
    echo "=== Remote Branches ==="
    if [[ -n "$filter" ]]; then
        git branch -r | grep -i "$filter"
    else
        git branch -r
    fi
    
    echo ""
    echo "Current branch: $(git branch --show-current)"
}


# Create and switch to new branch
# git_branch_new feature/login main

git_branch_new() {
    local branch_name="$1"
    local from_branch="$2"
    
    if [[ -z "$branch_name" ]]; then
        echo "Error: Branch name required" >&2
        return 1
    fi
    
    if [[ -n "$from_branch" ]]; then
        git checkout -b "$branch_name" "$from_branch"
    else
        git checkout -b "$branch_name"
    fi
    
    echo " Created and switched to: $branch_name"
    echo ""
    echo "Push with: git push -u origin $branch_name"
}


# git_branch_delete - Delete branch locally and remotely
#  git_branch_delete <branch_name> [remote]
# Example: git_branch_delete feature/old-feature origin

git_branch_delete() {
    local branch_name="$1"
    local remote="$2"
    
    if [[ -z "$branch_name" ]]; then
        echo "Error: Branch name required" >&2
        return 1
    fi
    
    local current=$(git branch --show-current)
    if [[ "$branch_name" == "$current" ]]; then
        echo "Error: Cannot delete current branch. Switch first." >&2
        return 1
    fi
    
    echo "Deleting branch: $branch_name"
    
    # Delete local
    if git branch -d "$branch_name" 2>/dev/null; then
        echo " Local branch deleted"
    elif git branch -D "$branch_name" 2>/dev/null; then
        echo " Local branch force-deleted (had unmerged changes)"
    else
        echo "  Local branch doesn't exist or already deleted"
    fi
    
    # Delete remote if specified
    if [[ -n "$remote" ]]; then
        if git push "$remote" --delete "$branch_name" 2>/dev/null; then
            echo " Remote branch deleted from $remote"
        else
            echo "  Remote branch doesn't exist or already deleted"
        fi
    fi
}


# 22. git_branch_rename - Rename current or specified branch
#  git_branch_rename <new_name> [old_name]
# Example: git_branch_rename feature/better-name

git_branch_rename() {
    local new_name="$1"
    local old_name="$2"
    
    if [[ -z "$new_name" ]]; then
        echo "Error: New branch name required" >&2
        return 1
    fi
    
    if [[ -n "$old_name" ]]; then
        git branch -m "$old_name" "$new_name"
    else
        old_name=$(git branch --show-current)
        git branch -m "$new_name"
    fi
    
    echo " Renamed '$old_name' to '$new_name'"
    echo ""
    echo "If pushed, update remote with:"
    echo "  git push origin :$old_name"
    echo "  git push -u origin $new_name"
}


# 23. git_branch_cleanup - Delete merged branches
#  git_branch_cleanup [main_branch]
# Example: git_branch_cleanup main

git_branch_cleanup() {
    local main_branch="${1:-main}"
    
    # Make sure we're on main
    git checkout "$main_branch" 2>/dev/null || git checkout master 2>/dev/null
    
    # Fetch and prune
    git fetch --prune
    
    echo "Branches merged into $main_branch:"
    local merged=$(git branch --merged | grep -v "^\*" | grep -v "$main_branch" | grep -v "master")
    
    if [[ -z "$merged" ]]; then
        echo "  No merged branches to clean up"
        return 0
    fi
    
    echo "$merged" | sed 's/^/  /'
    echo ""
    
    read -p "Delete these branches? [Y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy] ]]; then
        echo "$merged" | xargs -r git branch -d
        echo " Merged branches deleted"
    fi
}


# 24. git_branch_track - Set up tracking for existing branch
#  git_branch_track [remote] [branch]
# Example: git_branch_track origin feature-branch

git_branch_track() {
    local remote="${1:-origin}"
    local branch="${2:-$(git branch --show-current)}"
    
    git branch --set-upstream-to="$remote/$branch" "$branch"
    echo " $branch now tracks $remote/$branch"
}


# ============================================================================
# GIT REBASING WORKFLOWS
# ============================================================================


# 25. git_rebase_onto - Rebase current branch onto another
#  git_rebase_onto <target_branch>
# Example: git_rebase_onto main

git_rebase_onto() {
    local target="${1:-main}"
    local current=$(git branch --show-current)
    
    echo "Rebasing '$current' onto '$target'..."
    echo ""
    
    # Show what will happen
    git fetch origin "$target" --quiet 2>/dev/null
    local commits=$(git rev-list --count "$target"..HEAD 2>/dev/null || echo "?")
    echo "Commits to rebase: $commits"
    git log "$target"..HEAD --oneline 2>/dev/null | head -10
    echo ""
    
    read -p "Continue with rebase? [Y/N]: " confirm
    if [[ "$confirm" =~ ^[Nn] ]]; then
        echo "Cancelled"
        return 0
    fi
    
    if git rebase "$target"; then
        echo ""
        echo " Rebase successful"
        echo "Push with: git push --force-with-lease"
    else
        echo ""
        echo "✗ Conflicts detected!"
        echo ""
        echo "Options:"
        echo "  1. Fix conflicts in files"
        echo "  2. git add <fixed-files>"
        echo "  3. git rebase --continue"
        echo ""
        echo "Or abort: git rebase --abort"
        return 1
    fi
}


# 26. git_rebase_interactive - Interactive rebase for cleaning history
#  git_rebase_interactive [num_commits]
# Example: git_rebase_interactive 5

git_rebase_interactive() {
    local num_commits="${1:-5}"
    
    echo "Last $num_commits commits:"
    git log --oneline -"$num_commits"
    echo ""
    
    echo "Interactive rebase options:"
    echo "  pick   = keep commit as-is"
    echo "  reword = change commit message"
    echo "  squash = merge into previous commit"
    echo "  fixup  = merge into previous (discard message)"
    echo "  drop   = remove commit"
    echo ""
    
    read -p "Start interactive rebase for $num_commits commits? [Y/N]: " confirm
    if [[ "$confirm" =~ ^[Yy] ]]; then
        git rebase -i HEAD~"$num_commits"
    fi
}


# 27. git_rebase_abort - Safely abort a rebase in progress
#  git_rebase_abort

git_rebase_abort() {
    if [[ -d "$(git rev-parse --git-dir)/rebase-merge" ]] || \
       [[ -d "$(git rev-parse --git-dir)/rebase-apply" ]]; then
        git rebase --abort
        echo " Rebase aborted"
        echo "You're back on: $(git branch --show-current)"
    else
        echo "No rebase in progress"
    fi
}


# 28. git_rebase_continue - Continue rebase after fixing conflicts
#  git_rebase_continue

git_rebase_continue() {
    # Check for conflict markers
    if grep -rn "<<<<<<< HEAD" . --include="*" 2>/dev/null | head -1; then
        echo "!!!Conflict markers still present in files!!!"
        echo "Fix conflicts first, then run this command again"
        return 1
    fi
    
    git add -A
    git rebase --continue
}


# ============================================================================
# GIT MERGING WORKFLOWS
# ============================================================================


# 29. git_merge_branch - Merge branch with options
#  git_merge_branch <source_branch> [--squash|--no-ff]
# Example: git_merge_branch feature/login --squash

git_merge_branch() {
    local source_branch="$1"
    local merge_type="$2"
    
    if [[ -z "$source_branch" ]]; then
        echo "Error: Source branch required" >&2
        return 1
    fi
    
    local current=$(git branch --show-current)
    echo "Merging '$source_branch' into '$current'..."
    
    # Show what will be merged
    echo ""
    echo "Commits to merge:"
    git log "$current".."$source_branch" --oneline 2>/dev/null | head -10
    echo ""
    
    case "$merge_type" in
        --squash)
            echo "Mode: Squash (combine all commits into one)"
            git merge --squash "$source_branch"
            echo ""
            echo "Changes staged. Commit with: git commit"
            ;;
        --no-ff)
            echo "Mode: No fast-forward (always create merge commit)"
            git merge --no-ff "$source_branch"
            ;;
        *)
            echo "Mode: Standard merge"
            git merge "$source_branch"
            ;;
    esac
}


# 30. git_merge_abort - Abort a merge in progress
#  git_merge_abort

git_merge_abort() {
    if git merge --abort 2>/dev/null; then
        echo " Merge aborted"
    else
        echo "No merge in progress"
    fi
}


# 31. git_merge_and_delete - Merge branch and delete it
#  git_merge_and_delete <branch_to_merge> [target_branch]
# Example: git_merge_and_delete feature/login main

git_merge_and_delete() {
    local source_branch="$1"
    local target_branch="${2:-$(git branch --show-current)}"
    
    if [[ -z "$source_branch" ]]; then
        echo "Error: Branch to merge required" >&2
        return 1
    fi
    
    # Switch to target if needed
    local current=$(git branch --show-current)
    if [[ "$current" != "$target_branch" ]]; then
        echo "Switching to $target_branch..."
        git checkout "$target_branch"
    fi
    
    # Pull latest
    echo "Pulling latest $target_branch..."
    git pull origin "$target_branch"
    
    # Merge
    echo ""
    echo "Merging $source_branch..."
    if git merge --no-ff "$source_branch" -m "Merge branch '$source_branch' into $target_branch"; then
        echo " Merge successful"
        
        # Push
        read -p "Push merged changes? [Y/N]: " push_confirm
        if [[ ! "$push_confirm" =~ ^[Nn] ]]; then
            git push origin "$target_branch"
        fi
        
        # Delete branch
        echo ""
        read -p "Delete '$source_branch' locally and remotely? [Y/N]: " delete_confirm
        if [[ ! "$delete_confirm" =~ ^[Nn] ]]; then
            git branch -d "$source_branch"
            git push origin --delete "$source_branch" 2>/dev/null
            echo " Branch '$source_branch' deleted"
        fi
    else
        echo "✗ Merge failed - resolve conflicts first"
        return 1
    fi
}


# 32. git_merge_dry_run - Preview merge without doing it
#  git_merge_dry_run <source_branch>
# Example: git_merge_dry_run feature/login

git_merge_dry_run() {
    local source_branch="$1"
    
    if [[ -z "$source_branch" ]]; then
        echo "Error: Source branch required" >&2
        return 1
    fi
    
    local current=$(git branch --show-current)
    
    echo "=== Merge Preview: $source_branch → $current ==="
    echo ""
    
    # Check for conflicts
    echo "Checking for conflicts..."
    if git merge --no-commit --no-ff "$source_branch" &>/dev/null; then
        echo " No conflicts detected"
        git merge --abort
    else
        echo "⚠️  Conflicts would occur in:"
        git diff --name-only --diff-filter=U
        git merge --abort
    fi
    
    echo ""
    echo "Files that would change:"
    git diff --stat "$current"..."$source_branch"
    
    echo ""
    echo "Commits that would be merged:"
    git log "$current".."$source_branch" --oneline
}


# ============================================================================
# GIT WEBHOOK EXAMPLES & HELPERS
# ============================================================================






# ============================================================================
# HELPER: List all available functions
# ============================================================================
git_helpers_list() {
    cat << 'EOF'
GIT HELPERS - Available Functions
=================================

INITIALIZATION:
  git_init_new [dir]                  - Initialize new repo with .gitignore & README
  git_connect_remote <url> [name]     - Connect to remote origin
  git_clone_setup <url> [dir]         - Clone and show repo info

PULLING:
  git_pull_safe [remote] [branch]     - Pull with auto-stash
  git_pull_rebase [remote] [branch]   - Pull with rebase
  git_fetch_all                       - Fetch all remotes, prune dead branches

STAGING:
  git_stage_interactive               - Interactive staging menu
  git_stage_by_type <ext>             - Stage files by extension
  git_unstage [files...]              - Unstage files
  git_discard [files...]              - Discard changes (dangerous!)

COMMITTING:
  git_commit_conventional             - Commit with conventional format
  git_commit_quick <message>          - Quick commit
  git_commit_amend [message]          - Amend last commit
  git_commit_fixup <hash>             - Create fixup commit
  git_wip [message]                   - Quick WIP commit

PUSHING:
  git_push_safe [remote] [branch]     - Push with checks
  git_push_force_safe [remote] [br]   - Force push with lease
  git_push_tags [remote]              - Push all tags

BRANCHES:
  git_branches [filter]               - List branches with info
  git_branch_new <name> [from]        - Create and switch to branch
  git_branch_delete <name> [remote]   - Delete local/remote branch
  git_branch_rename <new> [old]       - Rename branch
  git_branch_cleanup [main]           - Delete merged branches
  git_branch_track [remote] [branch]  - Set up tracking

REBASING:
  git_rebase_onto <target>            - Rebase onto branch
  git_rebase_interactive [n]          - Interactive rebase last n commits
  git_rebase_abort                    - Abort rebase
  git_rebase_continue                 - Continue after fixing conflicts

MERGING:
  git_merge_branch <src> [--squash]   - Merge with options
  git_merge_abort                     - Abort merge
  git_merge_and_delete <branch>       - Merge and cleanup branch
  git_merge_dry_run <branch>          - Preview merge

WEBHOOKS:
  git_webhook_payload <event>         - Generate sample payload
  git_webhook_test <url> [event]      - Test webhook endpoint
  git_webhook_server [port]           - Start test receiver
  git_webhook_script <lang>           - Generate handler template

STATUS & INFO:
  git_status_full                     - Comprehensive status
  git_log_pretty [n]                  - Pretty log output
  git_aliases                         - Show git aliases
  git_undo [n]                        - Undo last n commits

Run: git_helpers_list  - Show this help
EOF
}

echo "Git helpers loaded. Run 'git_helpers_list' to see available functions."