#!/bin/bash
# service_status - Check status of a service with details
service_status() {
    local service="$1"
    
    if [[ -z "$service" ]]; then
        echo "Error: Service name required" >&2
        return 1
    fi
    
    echo "=== Service: $service ==="
    
    if systemctl is-active --quiet "$service" 2>/dev/null; then
        echo "Status: RUNNING"
    else
        echo "Status: STOPPED"
    fi
    
    if systemctl is-enabled --quiet "$service" 2>/dev/null; then
        echo "Enabled: YES (starts on boot)"
    else
        echo "Enabled: NO"
    fi
    
    echo ""
    echo "=== Recent Logs ==="
    journalctl -u "$service" -n 10 --no-pager 2>/dev/null || \
        echo "No journal logs available"
    
    echo ""
    echo "=== Full Status ==="
    systemctl status "$service" --no-pager 2>/dev/null || \
        service "$service" status 2>/dev/null || \
        echo "Service not found or status unavailable"
}


# restart_service - Safely restart a service with status check
restart_service() {
    local service="$1"
    
    if [[ -z "$service" ]]; then
        echo "Error: Service name required" >&2
        return 1
    fi
    
    echo "Restarting $service..."
    
    if sudo systemctl restart "$service" 2>/dev/null; then
        sleep 2
        if systemctl is-active --quiet "$service"; then
            echo "✓ $service restarted successfully and is running"
            return 0
        else
            echo "✗ $service restarted but is not running!"
            echo "Check logs with: journalctl -u $service -n 50"
            return 1
        fi
    else
        echo "✗ Failed to restart $service"
        return 1
    fi
}


# list_services - List all services with their status
list_services() {
    local filter="${1:-all}"
    
    case "$filter" in
        running)
            echo "=== Running Services ==="
            systemctl list-units --type=service --state=running --no-pager
            ;;
        enabled)
            echo "=== Enabled Services ==="
            systemctl list-unit-files --type=service --state=enabled --no-pager
            ;;
        failed)
            echo "=== Failed Services ==="
            systemctl list-units --type=service --state=failed --no-pager
            ;;
        all|*)
            echo "=== All Services ==="
            systemctl list-units --type=service --no-pager
            ;;
    esac
}


# watch_service_logs - Watch service logs in real-time
watch_service_logs() {
    local service="$1"
    local num_lines="${2:-20}"
    
    if [[ -z "$service" ]]; then
        echo "Error: Service name required" >&2
        return 1
    fi
    
    echo "Watching logs for $service (Ctrl+C to stop)..."
    journalctl -u "$service" -n "$num_lines" -f
}


# toggle_service - Enable/disable service on boot
toggle_service() {
    local service="$1"
    local action="$2"
    
    if [[ -z "$service" || -z "$action" ]]; then
        echo "Error: service_name and action (enable/disable) required" >&2
        return 1
    fi
    
    case "$action" in
        enable)
            if sudo systemctl enable "$service" 2>/dev/null; then
                echo "✓ $service enabled - will start on boot"
                return 0
            fi
            ;;
        disable)
            if sudo systemctl disable "$service" 2>/dev/null; then
                echo "✓ $service disabled - will not start on boot"
                return 0
            fi
            ;;
        *)
            echo "Error: action must be 'enable' or 'disable'" >&2
            return 1
            ;;
    esac
    
    echo "✗ Failed to $action $service" >&2
    return 1
}