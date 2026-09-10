#!/usr/bin/env python3
"""
Superset Dashboard Import & Bootstrap Script
============================================
Imports the exact, fully-styled Credit Card Fraud Detection dashboard export
into Apache Superset and configures the PostgreSQL serving database connection
using environment variables.

Idempotent: safe to run multiple times without creating duplicate objects.
"""

import os
import sys
import zipfile
from pathlib import Path
from flask import g
from superset.app import create_app

app = create_app()


def import_fraud_dashboard():
    with app.app_context():
        from superset import db, security_manager
        from superset.models.core import Database
        from superset.models.slice import Slice
        from superset.commands.dashboard.importers.v1 import ImportDashboardsCommand

        print("Setting admin context for Superset import...")
        admin_username = os.environ.get("SUPERSET_ADMIN_USER", "admin")
        admin_user = security_manager.find_user(admin_username)
        if not admin_user:
            print(f"Error: Admin user '{admin_username}' not found in Superset security manager.")
            sys.exit(1)
        g.user = admin_user

        # 1. Prepare contents dict for ImportDashboardsCommand
        contents = {}

        zip_path = Path("/app/dashboard_export.zip")
        dir_path = Path("/app/dashboard_export")

        if zip_path.exists():
            print(f"Loading export from zip: {zip_path}")
            with zipfile.ZipFile(zip_path, "r") as z:
                for name in z.namelist():
                    parts = name.split("/", 1)
                    rel_path = parts[1] if len(parts) > 1 else parts[0]
                    if rel_path:
                        contents[rel_path] = z.read(name)
        elif dir_path.exists():
            print(f"Loading export from directory: {dir_path}")
            for p in dir_path.rglob("*"):
                if p.is_file():
                    rel_path = str(p.relative_to(dir_path))
                    contents[rel_path] = p.read_bytes()
        else:
            print("Error: Neither /app/dashboard_export.zip nor /app/dashboard_export found.")
            sys.exit(1)

        # 2. Run import command with overwrite=True
        print("Importing dashboard metadata via Superset v1 importer...")
        cmd = ImportDashboardsCommand(contents, overwrite=True)
        cmd.run()
        print("Dashboard metadata imported successfully.")

        # Cleanup orphan "Transactions Over Time" slice if present from older versions
        tot_slice = db.session.query(Slice).filter(
            (Slice.slice_name == "Transactions Over Time") | (Slice.id == 8)
        ).first()
        if tot_slice:
            db.session.delete(tot_slice)
            db.session.commit()
            print("Removed orphan 'Transactions Over Time' slice from database.")

        # 3. Ensure database connection 'Fraud Serving DB' uses dynamic URI from env vars
        pg_user = os.environ.get("POSTGRES_DW_USER", "fraud_etl")
        pg_pass = os.environ.get("POSTGRES_DW_PASSWORD", "fraud_etl")
        pg_db = os.environ.get("POSTGRES_DW_DB", "frauddb")
        pg_host = os.environ.get("POSTGRES_DW_HOST", "postgres-dw")
        pg_port = os.environ.get("POSTGRES_DW_PORT_INT", "5432")

        uri = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        db_name = "Fraud Serving DB"

        db_obj = db.session.query(Database).filter_by(database_name=db_name).first()
        if db_obj:
            db_obj.sqlalchemy_uri = uri
            db.session.commit()
            print(f"Updated '{db_name}' connection URI to: {uri}")
        else:
            db_obj = Database(database_name=db_name, sqlalchemy_uri=uri)
            db.session.add(db_obj)
            db.session.commit()
            print(f"Created '{db_name}' connection -> {uri}")

        print("Superset dashboard import and database configuration completed successfully.")


if __name__ == "__main__":
    import_fraud_dashboard()
