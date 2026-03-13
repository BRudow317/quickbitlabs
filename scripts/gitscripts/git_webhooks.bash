#!/bin/bash
# git_webhook_payload - Generate sample webhook payload
#  git_webhook_payload <event_type>
# Example: git_webhook_payload push

git_webhook_payload() {
    local event_type="${1:-push}"
    local repo_name=$(basename "$(git rev-parse --show-toplevel 2>/dev/null)" 2>/dev/null || echo "my-repo")
    local branch=$(git branch --show-current 2>/dev/null || echo "main")
    local commit=$(git rev-parse HEAD 2>/dev/null || echo "abc123def456")
    local author=$(git config user.name || echo "Developer")
    local email=$(git config user.email || echo "dev@example.com")
    
    case "$event_type" in
        push)
            cat << EOF
{
  "event": "push",
  "repository": {
    "name": "$repo_name",
    "full_name": "organization/$repo_name",
    "url": "https://github.com/organization/$repo_name"
  },
  "ref": "refs/heads/$branch",
  "before": "0000000000000000000000000000000000000000",
  "after": "$commit",
  "pusher": {
    "name": "$author",
    "email": "$email"
  },
  "commits": [
    {
      "id": "$commit",
      "message": "$(git log -1 --pretty=%B 2>/dev/null || echo 'Sample commit message')",
      "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
      "author": {
        "name": "$author",
        "email": "$email"
      }
    }
  ]
}
EOF
            ;;
        pull_request)
            cat << EOF
{
  "event": "pull_request",
  "action": "opened",
  "number": 42,
  "pull_request": {
    "title": "Feature: Add new functionality",
    "body": "This PR adds new features...",
    "head": {
      "ref": "$branch",
      "sha": "$commit"
    },
    "base": {
      "ref": "main"
    },
    "user": {
      "login": "$author"
    },
    "mergeable": true,
    "merged": false
  },
  "repository": {
    "name": "$repo_name",
    "full_name": "organization/$repo_name"
  }
}
EOF
            ;;
        release)
            cat << EOF
{
  "event": "release",
  "action": "published",
  "release": {
    "tag_name": "v1.0.0",
    "name": "Release v1.0.0",
    "body": "## Changes\\n- Feature 1\\n- Bug fix 2",
    "draft": false,
    "prerelease": false,
    "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "author": {
      "login": "$author"
    }
  },
  "repository": {
    "name": "$repo_name",
    "full_name": "organization/$repo_name"
  }
}
EOF
            ;;
        *)
            echo "Available event types: push, pull_request, release"
            return 1
            ;;
    esac
}


# 34. git_webhook_test - Test webhook endpoint with sample payload
#  git_webhook_test <webhook_url> [event_type] [secret]
# Example: git_webhook_test https://api.example.com/webhook push mysecret

git_webhook_test() {
    local webhook_url="$1"
    local event_type="${2:-push}"
    local secret="$3"
    
    if [[ -z "$webhook_url" ]]; then
        echo "Error: Webhook URL required" >&2
        echo "Usage: git_webhook_test <url> [event_type] [secret]" >&2
        return 1
    fi
    
    local payload=$(git_webhook_payload "$event_type")
    
    echo "Testing webhook: $webhook_url"
    echo "Event type: $event_type"
    echo ""
    
    local curl_opts=(-X POST -H "Content-Type: application/json")
    curl_opts+=(-H "X-GitHub-Event: $event_type")
    
    # Add signature if secret provided
    if [[ -n "$secret" ]]; then
        local signature=$(echo -n "$payload" | openssl dgst -sha256 -hmac "$secret" | cut -d' ' -f2)
        curl_opts+=(-H "X-Hub-Signature-256: sha256=$signature")
        echo "Signature: sha256=$signature"
    fi
    
    echo ""
    echo "Sending payload..."
    echo ""
    
    curl "${curl_opts[@]}" -d "$payload" "$webhook_url" -w "\nHTTP Status: %{http_code}\n"
}


# 35. git_webhook_server - Start simple webhook receiver for testing
#  git_webhook_server [port]
# Example: git_webhook_server 8080
# Requires: Python 3

git_webhook_server() {
    local port="${1:-8080}"
    
    echo "Starting webhook test server on port $port..."
    echo "Press Ctrl+C to stop"
    echo ""
    echo "Send webhooks to: http://localhost:$port/webhook"
    echo ""
    
    python3 << EOF
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        
        print("\n" + "="*60)
        print(f"Received webhook at {self.path}")
        print("="*60)
        print("\nHeaders:")
        for header, value in self.headers.items():
            print(f"  {header}: {value}")
        
        print("\nPayload:")
        try:
            payload = json.loads(body)
            print(json.dumps(payload, indent=2))
        except:
            print(body.decode())
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status": "received"}')
    
    def log_message(self, format, *args):
        pass  # Suppress default logging

print(f"Webhook server running on http://localhost:$port")
HTTPServer(('', $port), WebhookHandler).serve_forever()
EOF
}


# 36. git_webhook_script - Generate webhook handler script template
#  git_webhook_script <language>
# Example: git_webhook_script bash

git_webhook_script() {
    local language="${1:-bash}"
    
    case "$language" in
        bash)
            cat << 'SCRIPT'
#!/bin/bash
# Git Webhook Handler - Bash
# Deploy: Set up with a web server like nginx + fcgiwrap

# Read the payload
read -r PAYLOAD

# Parse event type from header (set by web server)
EVENT_TYPE="${HTTP_X_GITHUB_EVENT:-push}"

# Log the event
echo "[$(date)] Received $EVENT_TYPE event" >> /var/log/webhook.log

# Verify signature (if secret is set)
SECRET="your-webhook-secret"
if [[ -n "$HTTP_X_HUB_SIGNATURE_256" ]]; then
    EXPECTED="sha256=$(echo -n "$PAYLOAD" | openssl dgst -sha256 -hmac "$SECRET" | cut -d' ' -f2)"
    if [[ "$HTTP_X_HUB_SIGNATURE_256" != "$EXPECTED" ]]; then
        echo "Invalid signature"
        exit 1
    fi
fi

# Handle different events
case "$EVENT_TYPE" in
    push)
        BRANCH=$(echo "$PAYLOAD" | jq -r '.ref' | sed 's|refs/heads/||')
        if [[ "$BRANCH" == "main" ]]; then
            cd /var/www/myapp
            git pull origin main
            # Run deployment commands
            ./deploy.sh
        fi
        ;;
    pull_request)
        ACTION=$(echo "$PAYLOAD" | jq -r '.action')
        echo "PR action: $ACTION"
        ;;
esac

echo "OK"
SCRIPT
            ;;
        node|nodejs|javascript)
            cat << 'SCRIPT'
// Git Webhook Handler - Node.js
// Run: node webhook-server.js

const http = require('http');
const crypto = require('crypto');
const { execSync } = require('child_process');

const SECRET = process.env.WEBHOOK_SECRET || 'your-webhook-secret';
const PORT = process.env.PORT || 3000;

function verifySignature(payload, signature) {
    const expected = 'sha256=' + crypto
        .createHmac('sha256', SECRET)
        .update(payload)
        .digest('hex');
    return crypto.timingSafeEqual(
        Buffer.from(signature),
        Buffer.from(expected)
    );
}

const server = http.createServer((req, res) => {
    if (req.method !== 'POST' || req.url !== '/webhook') {
        res.writeHead(404);
        return res.end('Not found');
    }

    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
        // Verify signature
        const signature = req.headers['x-hub-signature-256'];
        if (signature && !verifySignature(body, signature)) {
            res.writeHead(401);
            return res.end('Invalid signature');
        }

        const event = req.headers['x-github-event'];
        const payload = JSON.parse(body);

        console.log(`[${new Date().toISOString()}] ${event} event received`);

        // Handle events
        switch (event) {
            case 'push':
                const branch = payload.ref.replace('refs/heads/', '');
                if (branch === 'main') {
                    console.log('Deploying main branch...');
                    try {
                        execSync('cd /var/www/app && git pull && npm install && pm2 restart app');
                        console.log('Deployment successful');
                    } catch (err) {
                        console.error('Deployment failed:', err.message);
                    }
                }
                break;
            case 'pull_request':
                console.log(`PR ${payload.action}: #${payload.number}`);
                break;
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok' }));
    });
});

server.listen(PORT, () => {
    console.log(`Webhook server running on port ${PORT}`);
});
SCRIPT
            ;;
        python)
            cat << 'SCRIPT'
#!/usr/bin/env python3
"""Git Webhook Handler - Python (Flask)
Install: pip install flask
Run: python webhook-server.py
"""

import hmac
import hashlib
import subprocess
from flask import Flask, request, jsonify

app = Flask(__name__)
SECRET = b'your-webhook-secret'

def verify_signature(payload, signature):
    if not signature:
        return False
    expected = 'sha256=' + hmac.new(SECRET, payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected)

@app.route('/webhook', methods=['POST'])
def webhook():
    # Verify signature
    signature = request.headers.get('X-Hub-Signature-256')
    if not verify_signature(request.data, signature):
        return jsonify({'error': 'Invalid signature'}), 401
    
    event = request.headers.get('X-GitHub-Event', 'unknown')
    payload = request.json
    
    print(f"Received {event} event")
    
    # Handle events
    if event == 'push':
        branch = payload['ref'].replace('refs/heads/', '')
        if branch == 'main':
            print('Deploying main branch...')
            try:
                subprocess.run(
                    ['bash', '-c', 'cd /var/www/app && git pull && ./deploy.sh'],
                    check=True
                )
                print('Deployment successful')
            except subprocess.CalledProcessError as e:
                print(f'Deployment failed: {e}')
    
    elif event == 'pull_request':
        action = payload['action']
        pr_number = payload['number']
        print(f'PR {action}: #{pr_number}')
    
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
SCRIPT
            ;;
        *)
            echo "Available languages: bash, node, python"
            return 1
            ;;
    esac
}