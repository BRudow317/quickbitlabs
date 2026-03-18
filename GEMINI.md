Recommended Safety Mandates for GEMINI.md

1. The Backup Rule (Recovery):
    "Before modifying or overwriting any file, you MUST create a backup copy with a .bak suffix (e.g., plan.md ->
plan.md.bak). This provides an immediate recovery path for untracked files."

2. The Surgical Edit Rule (Prevention):
    "You are prohibited from using write_file on any existing file path. You MUST use read_file to confirm content,        
followed by replace for surgical updates. This prevents 'blind overwriting' of work."

3. The Audit Log Rule (Auditability):
    "Maintain a root-level CHANGELOG_SESSION.md. For every filesystem modification, you MUST log the timestamp, the tool
used, the file path, and a one-sentence rationale BEFORE executing the change."

4. The Staging Rule (Auditability & Recovery):
    "After every successful file modification, you MUST run git add <file_path> to stage the change. This allows you to use
git diff --staged for instant audit and git restore --staged --worktree <file_path> for instant recovery, without needing
to commit."

5. The Confirmation Gate:
    "You MUST stop and wait for a 'Proceed' command after presenting a Research or Strategy summary before entering the    
Execution phase. Never assume a goal is a directive to act across multiple directories."
