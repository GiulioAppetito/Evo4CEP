#!/bin/bash

# Path to the docker-compose file (relative to the script location)
DOCKER_COMPOSE_FILE="$(dirname "$0")/../docker-compose.yml"

# Function to display usage instructions
usage() {
    echo "Usage:"
    echo "  ./manage-architecture.sh --start     # Start architecture"
    echo "  ./manage-architecture.sh --stop      # Stop architecture"
    echo "  ./manage-architecture.sh --restart   # Restart architecture"
    echo "  ./manage-architecture.sh --rebuild   # Rebuild and start architecture"
}

# Check if Docker is installed and running
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed. Please install it and try again."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        echo "❌ Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Start Docker containers
run_docker_compose() {
    echo "🚀 Starting up architecture ..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" up -d
}

# Stop Docker containers
stop_docker_compose() {
    echo "🛑 Stopping architecture ..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" down --remove-orphans
}

# Rebuild and start Docker containers
rebuild_docker_compose() {
    echo "🔄 Rebuilding and starting architecture ..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" up --build -d
}

# Handle script arguments
execute() {
    check_docker  # Ensure Docker is available
    case "$1" in
        --help) usage ;;
        --start) run_docker_compose ;;
        --stop) stop_docker_compose ;;
        --restart)
            stop_docker_compose
            sleep 5
            run_docker_compose
            ;;
        --rebuild) rebuild_docker_compose ;;
        *) usage ;;
    esac
}

# Execute the command passed to the script
execute "$@"
