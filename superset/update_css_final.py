#!/usr/bin/env python3
from superset.app import create_app

app = create_app()

def run():
    with app.app_context():
        from superset import db
        from superset.models.dashboard import Dashboard

        dash = db.session.query(Dashboard).filter_by(slug="credit-card-fraud-analytics").first()
        if dash:
            dash.css += """
            /* ========================================= */
            /* EXTRA FIX: CHART TITLES (FORCE WHITE)     */
            /* ========================================= */
            .chart-title, 
            .chart-header-title, 
            .header-title, 
            .header-title span,
            .dashboard-component-chart-title,
            .dashboard-component-header h1,
            .dashboard-component-header h2,
            .dashboard-component-header h3,
            .dashboard-component-header h4,
            .dashboard-component-header h5,
            .dashboard-component-header h6,
            .dashboard-component-header .header-title,
            .dashboard-component-header span,
            .slice_name, 
            .editable-title input,
            .dashboard-header .dashboard-header-title,
            .chart-header .header-title {
                color: #FFFFFF !important;
                font-weight: 700 !important;
                fill: #FFFFFF !important;
                text-shadow: none !important;
            }

            /* ========================================= */
            /* EXTRA FIX: WHITE DASHBOARD HEADER BAR     */
            /* ========================================= */
            .dashboard-header, 
            .header-with-actions, 
            .dashboard .header, 
            .dashboard-builder-header, 
            .dashboard-content-top,
            .dashboard-page-header,
            [data-test="dashboard-header"] {
                background-color: #111111 !important;
                background: #111111 !important;
                border-bottom: 1px solid #333333 !important;
            }
            """
            
            db.session.commit()
            print("Final CSS successfully appended!")
        else:
            print("Dashboard not found.")

if __name__ == "__main__":
    run()
