#!/usr/bin/env bash
# Runs once via the `superset-init` service: migrates Superset's own metadata
# store, creates the admin user, and pre-registers the postgres-dw serving
# database as a Superset "Database" connection so dashboards can be built
# against it immediately (Data -> Datasets in the UI).
set -e

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

echo "Registering the fraud serving database connection..."

python3 - <<'PYEOF'
import os

from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database

    uri = (
        f"postgresql+psycopg2://{os.environ.get('POSTGRES_DW_USER', 'fraud_etl')}:"
        f"{os.environ.get('POSTGRES_DW_PASSWORD', 'fraud_etl')}@postgres-dw:5432/"
        f"{os.environ.get('POSTGRES_DW_DB', 'frauddb')}"
    )

    name = "Fraud Serving DB"

    existing = (
        db.session.query(Database)
        .filter_by(database_name=name)
        .first()
    )

    if existing:
        print(f"'{name}' connection already exists, skipping.")
    else:
        database = Database(
            database_name=name,
            sqlalchemy_uri=uri
        )
        db.session.add(database)
        db.session.commit()
        print(f"Created '{name}' connection -> {uri}")
PYEOF

echo "Initializing datasets, charts, and dark theme dashboard..."
if [ -f "/app/import_dashboard.py" ]; then
    python3 /app/import_dashboard.py
else
    echo "Notice: /app/import_dashboard.py not mounted, skipping dashboard import."
fi

echo "Superset bootstrap complete."

