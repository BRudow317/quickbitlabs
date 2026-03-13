#!/bin/bash
# git_pull_safe [remote] [branch]
git_pull_safe() {
    local remote="${1:-origin}"
    local branch="${2:-$(git branch --show-current)}"
    
    echo "Pulling $remote/$branch safely..."
    
    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        echo "Stashing local changes..."
        git stash push -m "Auto-stash before pull $(date +%Y%m%d_%H%M%S)"
        local stashed=true
    fi
    
    # Fetch first to see what's coming
    git fetch "$remote" "$branch"
    
    local behind=$(git rev-list --count HEAD.."$remote/$branch" 2>/dev/null)
    local ahead=$(git rev-list --count "$remote/$branch"..HEAD 2>/dev/null)
    
    echo "Status: $behind commits behind, $ahead commits ahead"
    
    # Pull
    if git pull "$remote" "$branch"; then
        echo "✓ Pull successful"
    else
        echo "✗ Pull failed - you may have conflicts to resolve"
        return 1
    fi
    
    # Restore stash if we made one
    if [[ "$stashed" == true ]]; then
        echo "Restoring stashed changes..."
        if git stash pop; then
            echo "Stash restored"
        else
            echo "Stash conflicts - resolve manually"
            echo "  Your stash is preserved. Run: git stash show"
        fi
    fi
}


# Pull with rebase instead of merge
#  git_pull_rebase [remote] [branch]

git_pull_rebase() {
    local remote="${1:-origin}"
    local branch="${2:-$(git branch --show-current)}"
    
    echo "Pulling with rebase from $remote/$branch..."
    
    if git pull --rebase "$remote" "$branch"; then
        echo "✓ Pull rebase successful"
        echo "Your commits are now on top of $remote/$branch"
    else
        echo "✗ Rebase conflicts detected"
        echo ""
        echo "Options:"
        echo "  1. Fix conflicts, then: git add . && git rebase --continue"
        echo "  2. Abort rebase: git rebase --abort"
        return 1
    fi
}


# git_fetch_all - Fetch all remotes and prune dead branches
# git_fetch_all

git_fetch_all() {
    echo "Fetching all remotes..."
    git fetch --all --prune
    
    echo ""
    echo "Remote branches:"
    git branch -r
    
    echo ""
    echo "Local branches and their tracking status:"
    git branch -vv
}