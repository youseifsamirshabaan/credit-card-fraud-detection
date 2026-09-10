#!/usr/bin/env bash
# Runs once via the `superset-init` service: migrates Superset's own metadata
# store, creates the admin user, and imports the full fraud analytics dashboard.
set -e

echo "Waiting for postgres-dw serving database to be ready..."
python3 - <<'PYEOF'
import os
import socket
import time

host = "postgres-dw"
port = 5432
for i in range(30):
    try:
        with socket.create_connection((host, port), timeout=2):
            print("postgres-dw is reachable.")
            break
    except OSError:
        print(f"Waiting for postgres-dw:5432... ({i+1}/30)")
        time.sleep(2)
PYEOF

echo "Running Superset DB migrations..."
superset db upgrade

echo "Ensuring admin user exists..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname Admin \
    --lastname User \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || echo "Admin user already exists, continuing."

echo "Initializing Superset roles/permissions..."
superset init

echo "Importing fraud detection dashboard metadata and database connections..."
if [ -f "/app/import_dashboard.py" ]; then
    python3 /app/import_dashboard.py
else
    echo "Notice: /app/import_dashboard.py not mounted, skipping dashboard import."
fi

echo "Superset bootstrap complete."
