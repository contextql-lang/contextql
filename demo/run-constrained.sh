#!/usr/bin/env bash
set -euo pipefail

echo "ContextQL — Resource-Constrained Demo"
echo "Building and launching with CPU=0.5, RAM=128MB..."
echo ""

cd "$(dirname "$0")/.."
docker compose -f demo/docker-compose.constrained.yml up --build --abort-on-container-exit
