from superset.app import create_app
app = create_app()
with app.app_context():
    from superset import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    # Clean up dashboard_slices table directly
    db.session.execute("DELETE FROM dashboard_slices")
    db.session.execute("DELETE FROM dashboards")
    db.session.execute("DELETE FROM slices")
    db.session.commit()
    print("Database cleaned.")
