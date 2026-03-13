#!/bin/bash
git_init_new() {
local dir_name="${1:-.}"

if [[ "$dir_name" != "." ]]; then
    mkdir -p "$dir_name"
    cd "$dir_name" || return 1
fi



# Create the git ignore
cat > .gitignore << 'EOF'
# Auto generated .gitignore file

.DS_Store
Thumbs.db
desktop.ini
.idea/
.vscode/
*.swp
*.swo
*~

node_modules/
vendor/
__pycache__/
*.pyc


.env
.env.local
*.env
.env.development
.env.test
.env.production

dist/
build/
*.log
target/
/shelf/
/workspace.xml
_site/
.sass-cache/
.jekyll-cache/
.jekyll-metadata
/vendor
Gemfile.lock
*.code-workspace

bin/
lib/

EOF

# Create README.md
cat > README.md << EOF
# ${dir_name}

## Description

EOF

# Create Dockerfile
cat > Dockerfile << EOF
# ${dir_name}

FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app

# Copy Project, using .dockerignore to exclude files not needed for build
COPY . .

RUN chmod +x mvnw

RUN ./mvnw -B -DskipTests clean package 

# ---- Runtime stage: production container ----
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN addgroup --system spring && adduser --system --ingroup spring spring

# If there is exactly one bootable JAR, this wildcard is safe
COPY --from=build --chown=spring:spring /app/target/*.jar app.jar

EXPOSE 8080
USER spring:spring

ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport"

ENTRYPOINT ["java", "-jar", "app.jar"]

EOF

git init
git add .
git commit -m "Initial commit: project setup"

echo ""
echo " Repository initialized in: $(pwd)"
echo " .gitignore created"
echo " README.md created"
echo " Initial commit made"
echo ""
}


# Example: git_connect_remote https://github.com/user/repo.git
git_connect_remote() {
    local remote_url="$1"
    local remote_name="${2:-origin}"
    
    if [[ -z "$remote_url" ]]; then
        echo "Error: Remote URL required" >&2
        echo "Usage: git_connect_remote <url> [remote_name]" >&2
        return 1
    fi
    
    if ! git rev-parse --git-dir &>/dev/null; then
        echo "Error: Not in a git repository" >&2
        return 1
    fi
    
    # Check if remote already exists
    if git remote get-url "$remote_name" &>/dev/null; then
        echo "Remote '$remote_name' already exists. Updating URL..."
        git remote set-url "$remote_name" "$remote_url"
    else
        git remote add "$remote_name" "$remote_url"
    fi
    
    echo "✓ Remote '$remote_name' set to: $remote_url"
    echo ""
    echo "Push with: git push -u $remote_name $(git branch --show-current)"
}


# Example: git_clone_setup https://github.com/user/repo.git my_local_name
git_clone_setup() {
    local repo_url="$1"
    local directory="$2"
    
    if [[ -z "$repo_url" ]]; then
        echo "Error: Repository URL required" >&2
        return 1
    fi
    
    echo "Cloning repository..."
    if [[ -n "$directory" ]]; then
        git clone "$repo_url" "$directory"
        cd "$directory" || return 1
    else
        git clone "$repo_url"
        local repo_name=$(basename "$repo_url" .git)
        cd "$repo_name" || return 1
    fi
    
    echo ""
    echo "Repository info:"
    echo "  Branch: $(git branch --show-current)"
    echo "  Remote: $(git remote get-url origin)"
    echo "  Commits: $(git rev-list --count HEAD)"
    echo ""
    
    # Show available branches
    echo "Available branches:"
    git branch -a | head -20
}