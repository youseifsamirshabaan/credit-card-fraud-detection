#!/usr/bin/env python3
from superset.app import create_app

app = create_app()

def run():
    with app.app_context():
        from superset import db
        from superset.models.dashboard import Dashboard

        dash = db.session.query(Dashboard).filter_by(slug="credit-card-fraud-analytics").first()
        if dash:
            dash.css = """
            /* ========================================= */
            /* 1. TOP SUPERSET HEADER NAVIGATION         */
            /* ========================================= */
            .navbar, header, .top-nav, [role="navigation"], .ant-menu, .ant-menu-root, .ant-menu-horizontal {
                background-color: #111111 !important;
                background: #111111 !important;
                border-bottom: 1px solid #333333 !important;
                color: #FFFFFF !important;
            }
            .navbar a, .navbar .nav-link, .ant-menu-item, .ant-menu-item a, .ant-menu-submenu-title, header a {
                color: #E5E7EB !important;
            }
            .navbar a:hover, .navbar .nav-link:hover, .ant-menu-item:hover, .ant-menu-item a:hover, .ant-menu-submenu-title:hover, header a:hover {
                color: #FFFFFF !important;
            }
            /* Try to make the logo visible if it's dark by default */
            .navbar-brand img, header img {
                filter: brightness(0) invert(1) !important;
            }

            /* ========================================= */
            /* 2. BACKGROUNDS AND CONTAINERS             */
            /* ========================================= */
            body, .dashboard, .dashboard-content, .grid-container, .dashboard-content-editable, .dragdroppable, .dragdroppable-content, #app, .app {
                background-color: #111111 !important;
                background: #111111 !important;
                color: #FFFFFF !important;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
            }

            .dashboard-header {
                background-color: #111111 !important;
                background: #111111 !important;
                border-bottom: none !important;
                padding: 16px 24px !important;
            }
            .dashboard-component-chart-holder {
                background-color: #181818 !important;
                background: #181818 !important;
                border: 1px solid #333333 !important;
                border-radius: 8px !important;
                box-shadow: none !important;
                padding: 8px !important;
            }

            /* ========================================= */
            /* 3. TITLES AND HEADINGS                    */
            /* ========================================= */
            /* Dashboard Title */
            .dashboard-header .dashboard-header-title, .editable-title input {
                color: #FFFFFF !important;
                font-weight: 800 !important;
                font-size: 26px !important;
            }
            /* Chart and Section Titles */
            .chart-header .header-title, .header-title, .slice_name, h1, h2, h3, h4, h5, h6, .dashboard-markdown h1, .dashboard-markdown h2, .dashboard-markdown h3 {
                color: #FFFFFF !important;
                font-weight: 700 !important;
            }
            /* KPI Labels */
            .big_number_total .subheader-line, .big_number .subheader-line {
                color: #FFFFFF !important;
                font-weight: 600 !important;
            }
            /* KPI Values */
            .big_number_total .header-line, .big_number .header-line {
                color: #22D3EE !important; /* Bright cyan */
                font-size: 2.5rem !important;
                font-weight: 700 !important;
            }

            /* ========================================= */
            /* 4. CHART LABELS, LEGENDS, AXES            */
            /* ========================================= */
            text, tspan, .axis text, .tick text, g.tick text, .nv-axis text, .recharts-legend-item-text, .recharts-cartesian-axis-tick-value {
                fill: #E5E7EB !important;
                color: #E5E7EB !important;
            }
            .nv-legend text, .echarts-legend text, .legend text {
                fill: #FFFFFF !important;
                color: #FFFFFF !important;
            }

            /* ========================================= */
            /* 5. TABLES                                 */
            /* ========================================= */
            .table, .table th, .dashboard-component-tabs, .table-chart-table, .dt-bootstrap4 {
                background-color: #181818 !important;
                color: #FFFFFF !important;
            }
            .table th, .table-chart-table th, .dt-bootstrap4 th {
                background-color: #111111 !important;
                border-bottom: 2px solid #333333 !important;
                color: #FFFFFF !important;
                font-weight: 700 !important;
            }
            .table td, .table-chart-table td, .dt-bootstrap4 td {
                border-bottom: 1px solid #333333 !important;
                color: #E5E7EB !important;
            }
            .table-chart-table tbody tr:hover td, .dt-bootstrap4 tbody tr:hover td, .table tbody tr:hover td {
                background-color: #202020 !important;
                color: #FFFFFF !important;
            }

            /* ========================================= */
            /* 6. TOOLTIPS                               */
            /* ========================================= */
            .tooltip-inner, .nvtooltip, .echarts-tooltip, .ant-tooltip-inner {
                background-color: #202020 !important;
                color: #FFFFFF !important;
                border: 1px solid #333333 !important;
            }
            .nvtooltip text, .echarts-tooltip text {
                fill: #FFFFFF !important;
                color: #FFFFFF !important;
            }

            /* ========================================= */
            /* 7. FILTERS & FORMS                        */
            /* ========================================= */
            .filter-title, .ant-typography-secondary, .text-muted, .ant-form-item-label label, .filter-control-name {
                color: #E5E7EB !important;
            }
            .ant-select-selector, .Select__control, .ant-input {
                background-color: #202020 !important;
                border-color: #333333 !important;
                color: #FFFFFF !important;
            }
            .ant-select-selection-item, .Select__single-value, .ant-select-item-option-content {
                color: #FFFFFF !important;
            }
            .ant-select-dropdown, .Select__menu {
                background-color: #202020 !important;
                color: #FFFFFF !important;
            }

            /* Remove all black text globally in content */
            * {
                text-shadow: none !important;
            }
            """
            
            db.session.commit()
            print("CSS perfectly updated for full dark header and white texts.")
        else:
            print("Dashboard not found.")

if __name__ == "__main__":
    run()
