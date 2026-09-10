#!/usr/bin/env python3
import json
from superset.app import create_app

app = create_app()

def run():
    with app.app_context():
        from superset import db
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard

        # Fix charts
        slices = db.session.query(Slice).all()
        for slc in slices:
            params = json.loads(slc.params) if slc.params else {}
            query_context = json.loads(slc.query_context) if slc.query_context else None
            changed = False

            # Fix metrics in params
            if "metric" in params:
                m = params["metric"]
                if isinstance(m, dict) and m.get("expressionType") == "SQL":
                    if m["sqlExpression"] in ["total_transactions", "fraud_transactions", "fraud_rate_pct", "total_amount", "fraud_amount"]:
                        m["sqlExpression"] = f"SUM({m['sqlExpression']})"
                        changed = True

            if "metrics" in params:
                for m in params["metrics"]:
                    if isinstance(m, dict) and m.get("expressionType") == "SQL":
                        if m["sqlExpression"] in ["total_transactions", "fraud_transactions", "fraud_rate_pct", "total_amount", "fraud_amount"]:
                            m["sqlExpression"] = f"SUM({m['sqlExpression']})"
                            changed = True

            # Fix metrics in query_context
            if query_context and "queries" in query_context:
                for q in query_context["queries"]:
                    if "metrics" in q:
                        for m in q["metrics"]:
                            if isinstance(m, dict) and m.get("expressionType") == "SQL":
                                if m["sqlExpression"] in ["total_transactions", "fraud_transactions", "fraud_rate_pct", "total_amount", "fraud_amount"]:
                                    m["sqlExpression"] = f"SUM({m['sqlExpression']})"
                                    changed = True

            if changed:
                slc.params = json.dumps(params)
                if query_context:
                    slc.query_context = json.dumps(query_context)
                print(f"Fixed metric on slice: {slc.slice_name}")

        # Update CSS for visibility
        dash = db.session.query(Dashboard).filter_by(slug="credit-card-fraud-analytics").first()
        if dash:
            dash.css = """
            /* Backgrounds */
            body, .dashboard, .dashboard-content, .grid-container, .dashboard-content-editable, .dragdroppable, .dragdroppable-content {
                background-color: #111111 !important;
                color: #FFFFFF !important;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
            }

            /* Dashboard Header */
            .dashboard-header {
                background-color: #111111 !important;
                border-bottom: none !important;
                padding: 16px 24px !important;
            }
            .dashboard-header .dashboard-header-title {
                color: #FFFFFF !important;
                font-weight: 800 !important;
                font-size: 26px !important;
            }

            /* Card Backgrounds */
            .dashboard-component-chart-holder {
                background-color: #181818 !important;
                border: 1px solid #333333 !important;
                border-radius: 8px !important;
                box-shadow: none !important;
                padding: 8px !important;
            }

            /* Text Visibility overrides */
            .chart-header .header-title, .header-title, .slice_name {
                color: #FFFFFF !important;
                font-weight: 600 !important;
            }
            /* KPI */
            .big_number_total .header-line {
                color: #22D3EE !important;
                font-size: 2.5rem !important;
                font-weight: 700 !important;
            }
            .big_number_total .subheader-line {
                color: #FFFFFF !important;
            }

            /* Axis, legends, general text */
            text, tspan, .axis text, .tick text, g.tick text, .nv-axis text, .recharts-legend-item-text, .recharts-cartesian-axis-tick-value {
                fill: #E5E7EB !important;
                color: #E5E7EB !important;
            }
            .nv-legend text, .echarts-legend text, .legend text {
                fill: #FFFFFF !important;
            }

            /* Table */
            .table, .table th, .dashboard-component-tabs {
                background-color: #181818 !important;
                color: #FFFFFF !important;
            }
            .table th {
                border-bottom: 1px solid #333333 !important;
                color: #FFFFFF !important;
            }
            .table td {
                border-bottom: 1px solid #333333 !important;
                color: #E5E7EB !important;
            }
            .table-chart-table tbody tr:hover td, .dt-bootstrap4 tbody tr:hover td {
                background-color: #202020 !important;
            }

            /* Tooltips */
            .tooltip-inner, .nvtooltip, .echarts-tooltip {
                background-color: #202020 !important;
                color: #FFFFFF !important;
                border: 1px solid #333333 !important;
            }
            .nvtooltip text, .echarts-tooltip text {
                fill: #FFFFFF !important;
                color: #FFFFFF !important;
            }

            /* Filters */
            .filter-title, .ant-typography-secondary, .text-muted, .ant-form-item-label label {
                color: #E5E7EB !important;
            }
            .ant-select-selector, .Select__control {
                background-color: #202020 !important;
                border-color: #333333 !important;
                color: #FFFFFF !important;
            }
            .ant-select-selection-item {
                color: #FFFFFF !important;
            }
            """
            
            # Ensure label colors are set right
            meta = json.loads(dash.json_metadata) if dash.json_metadata else {}
            meta["label_colors"] = {
                "1": "#FF5A5F", "0": "#35C7C0",
                "Fraud Txns": "#FF5A5F", "Legit Txns": "#35C7C0",
                "Fraud Transactions": "#FF5A5F", "Total Transactions": "#35C7C0",
                "Fraud Rate %": "#FF5A5F", "Total Transaction Amount": "#35C7C0",
                "Fraud Amount": "#FF5A5F"
            }
            dash.json_metadata = json.dumps(meta)
            
            db.session.commit()
            print("CSS and label_colors updated.")

if __name__ == "__main__":
    run()
