import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-please-lab-only")

# Superset's own metadata store (dashboards, users, charts) - kept as SQLite
# on a named volume for lab simplicity. This is separate from postgres-dw,
# which is the *serving* database Superset connects to as a data source.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}
