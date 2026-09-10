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
            /* EXTRA FIX: PERMANENT WHITE TITLES         */
            /* ========================================= */
            .chart-title, 
            .chart-header-title, 
            .header-title, 
            .header-title span,
            .header-title a,
            .dashboard-component-chart-title,
            .dashboard-component-header h1,
            .dashboard-component-header h2,
            .dashboard-component-header h3,
            .dashboard-component-header h4,
            .dashboard-component-header h5,
            .dashboard-component-header h6,
            .dashboard-component-header .header-title,
            .dashboard-component-header span,
            .dashboard-component-header a,
            .slice_name, 
            .slice_name a,
            .editable-title input,
            .dashboard-header .dashboard-header-title,
            .chart-header .header-title,
            .chart-header .header-title a,
            .big_number_total .subheader-line, 
            .big_number .subheader-line,
            .dashboard-component-chart-holder a,
            .dashboard-component-chart-holder span {
                color: #FFFFFF !important;
                opacity: 1 !important;
                visibility: visible !important;
                -webkit-text-fill-color: #FFFFFF !important;
            }
            """
            
            db.session.commit()
            print("Final opacity CSS appended!")
        else:
            print("Dashboard not found.")

if __name__ == "__main__":
    run()
