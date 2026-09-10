#!/usr/bin/env python3
import json
import sys
from superset.app import create_app

app = create_app()

def build_query_context(datasource_id, queries):
    return json.dumps({
        "datasource": {"id": datasource_id, "type": "table"},
        "force": False,
        "queries": queries,
        "result_format": "json",
        "result_type": "full"
    })

def make_metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}

def make_groupby_query(datasource_id, groupby, metrics, time_range="No filter", row_limit=1000, order_desc=True):
    return build_query_context(datasource_id, [{
        "metrics": metrics,
        "columns": [],
        "groupby": groupby,
        "time_range": time_range,
        "row_limit": row_limit,
        "order_desc": order_desc
    }])

def make_kpi_query(datasource_id, metric_sql, metric_label):
    return build_query_context(datasource_id, [{
        "metrics": [make_metric(metric_sql, metric_label)],
        "columns": [],
        "groupby": [],
        "time_range": "No filter",
        "row_limit": 1
    }])

def make_timeseries_query(datasource_id, time_col, metrics, time_range="No filter"):
    return build_query_context(datasource_id, [{
        "metrics": metrics,
        "columns": [],
        "groupby": [],
        "granularity": time_col,
        "time_grain_sqla": "P1D",
        "time_range": time_range,
        "row_limit": 10000,
        "order_desc": False
    }])

def make_table_columns_query(datasource_id, columns, row_limit=50):
    return build_query_context(datasource_id, [{
        "metrics": [],
        "columns": columns,
        "groupby": [],
        "time_range": "No filter",
        "row_limit": row_limit,
        "order_desc": True
    }])

def build_position_json(kpis, charts):
    """
    Builds a robust Grid layout for Superset.
    kpis: list of 6 slices
    charts: list of 8 slices (will group by pairs)
    """
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]}
    }
    
    # KPIs Row (6 items, width 2 each = 12)
    row_kpi_id = "ROW_KPIS"
    pos["GRID_ID"]["children"].append(row_kpi_id)
    pos[row_kpi_id] = {"type": "ROW", "id": row_kpi_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    
    for i, slc in enumerate(kpis):
        chart_id = f"CHART_{slc.id}"
        pos[row_kpi_id]["children"].append(chart_id)
        pos[chart_id] = {
            "type": "CHART", "id": chart_id, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_kpi_id],
            "meta": {"width": 2, "height": 20, "chartId": slc.id, "sliceName": slc.slice_name}
        }
        
    # Pairs of charts (2 items per row)
    chart_pairs = [
        (charts[0], 4, charts[1], 8), # Fraud vs Legit (4) | Txns Over Time (8)
        (charts[2], 4, charts[3], 8), # Fraud Amt vs Legit Amt (4) | Fraud by Cat (8)
        (charts[4], 6, charts[5], 6), # Location (6) | Risk Level (6)
        (charts[6], 6, charts[7], 6), # Amt Dist (6) | Top Suspicious (6)
    ]
    
    for row_idx, (c1, w1, c2, w2) in enumerate(chart_pairs):
        row_id = f"ROW_CHART_{row_idx}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        
        ch1_id = f"CHART_{c1.id}"
        pos[row_id]["children"].append(ch1_id)
        pos[ch1_id] = {
            "type": "CHART", "id": ch1_id, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"width": w1, "height": 50, "chartId": c1.id, "sliceName": c1.slice_name}
        }
        
        ch2_id = f"CHART_{c2.id}"
        pos[row_id]["children"].append(ch2_id)
        pos[ch2_id] = {
            "type": "CHART", "id": ch2_id, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"width": w2, "height": 50, "chartId": c2.id, "sliceName": c2.slice_name}
        }
        
    return json.dumps(pos)

def run():
    with app.app_context():
        from superset import db
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard

        print("Fetching Fraud Serving DB connection...")
        database = db.session.query(Database).filter_by(database_name="Fraud Serving DB").first()
        if not database:
            print("ERROR: 'Fraud Serving DB' not found!")
            sys.exit(1)

        def get_ds(table_name):
            ds = db.session.query(SqlaTable).filter_by(database_id=database.id, table_name=table_name).first()
            if not ds:
                ds = SqlaTable(table_name=table_name, database=database, schema="public")
                db.session.add(ds)
                db.session.commit()
                ds.fetch_metadata()
                db.session.commit()
            return ds

        print("Loading datasets...")
        ds_txns = get_ds("transactions")
        ds_over_time = get_ds("vw_fraud_over_time")
        ds_top_susp = get_ds("vw_top_suspicious_transactions")
        ds_alerts = get_ds("alerts")
        ds_kpis = get_ds("vw_fraud_kpis")

        for ds, col in [(ds_txns, "event_time"), (ds_over_time, "txn_date"), (ds_alerts, "event_time")]:
            if ds.main_dttm_col != col:
                ds.main_dttm_col = col
                db.session.commit()

        # Safely clean old dashboard & slices
        for dash in db.session.query(Dashboard).filter((Dashboard.slug == "credit-card-fraud-analytics") | (Dashboard.dashboard_title == "Credit Card Fraud Detection — Real-Time Analytics")).all():
            dash.slices = []
            db.session.delete(dash)
        for slc in db.session.query(Slice).all():
            db.session.delete(slc)
        db.session.commit()

        def make_slice(name, viz_type, datasource, params, query_context_json=None):
            params["viz_type"] = viz_type
            params["datasource"] = f"{datasource.id}__table"
            slc = Slice(
                slice_name=name,
                viz_type=viz_type,
                datasource_type="table",
                datasource_id=datasource.id,
                params=json.dumps(params),
                query_context=query_context_json or ""
            )
            db.session.add(slc)
            db.session.flush()
            return slc

        # KPIs
        k1 = make_slice("Total Transactions", "big_number_total", ds_kpis, {"metric": make_metric("total_transactions", "Total Transactions"), "header_font_size": 0.5}, make_kpi_query(ds_kpis.id, "total_transactions", "Total Transactions"))
        k2 = make_slice("Fraud Transactions", "big_number_total", ds_kpis, {"metric": make_metric("fraud_transactions", "Fraud Transactions"), "header_font_size": 0.5}, make_kpi_query(ds_kpis.id, "fraud_transactions", "Fraud Transactions"))
        k3 = make_slice("Fraud Rate", "big_number_total", ds_kpis, {"metric": make_metric("fraud_rate_pct", "Fraud Rate %"), "header_font_size": 0.5}, make_kpi_query(ds_kpis.id, "fraud_rate_pct", "Fraud Rate %"))
        k4 = make_slice("Total Transaction Amount", "big_number_total", ds_kpis, {"metric": make_metric("total_amount", "Total Transaction Amount"), "header_font_size": 0.5}, make_kpi_query(ds_kpis.id, "total_amount", "Total Transaction Amount"))
        k5 = make_slice("Fraud Amount", "big_number_total", ds_kpis, {"metric": make_metric("fraud_amount", "Fraud Amount"), "header_font_size": 0.5}, make_kpi_query(ds_kpis.id, "fraud_amount", "Fraud Amount"))
        k6 = make_slice("Critical Alerts", "big_number_total", ds_alerts, {"metric": make_metric("COUNT(*)", "Critical Alerts"), "header_font_size": 0.5}, make_kpi_query(ds_alerts.id, "COUNT(*)", "Critical Alerts"))

        # Charts
        c1 = make_slice("Fraud vs Legitimate Transactions", "pie", ds_txns, {"groupby": ["is_fraud"], "metric": make_metric("COUNT(*)", "Transactions"), "donut": True, "show_legend": True, "labels_outside": True}, make_groupby_query(ds_txns.id, ["is_fraud"], [make_metric("COUNT(*)", "Transactions")]))
        c2 = make_slice("Transactions Over Time", "echarts_timeseries_line", ds_over_time, {"granularity_sqla": "txn_date", "time_range": "No filter", "metrics": [make_metric("total_transactions", "Total Transactions"), make_metric("fraud_transactions", "Fraud Transactions")], "x_axis_title": "Date", "y_axis_title": "Volume"}, make_timeseries_query(ds_over_time.id, "txn_date", [make_metric("total_transactions", "Total Transactions"), make_metric("fraud_transactions", "Fraud Transactions")]))
        c3 = make_slice("Fraud Amount vs Legitimate Amount", "dist_bar", ds_txns, {"groupby": ["is_fraud"], "metrics": [make_metric("SUM(transaction_amount)", "Total Amount")], "show_legend": False, "show_bar_value": True}, make_groupby_query(ds_txns.id, ["is_fraud"], [make_metric("SUM(transaction_amount)", "Total Amount")]))
        c4 = make_slice("Fraud by Merchant Category", "dist_bar", ds_txns, {"groupby": ["merchant_category"], "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")], "show_legend": False, "show_bar_value": True, "orientation": "horizontal"}, make_groupby_query(ds_txns.id, ["merchant_category"], [make_metric("SUM(is_fraud)", "Fraud Transactions")]))
        c5 = make_slice("Fraud by Merchant Location", "dist_bar", ds_txns, {"groupby": ["merchant_location"], "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")], "show_legend": False, "show_bar_value": True}, make_groupby_query(ds_txns.id, ["merchant_location"], [make_metric("SUM(is_fraud)", "Fraud Transactions")]))
        c6 = make_slice("Fraud by Risk Level", "dist_bar", ds_txns, {"groupby": ["merchant_risk_score"], "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")], "show_legend": False, "show_bar_value": True}, make_groupby_query(ds_txns.id, ["merchant_risk_score"], [make_metric("SUM(is_fraud)", "Fraud Transactions")]))
        c7 = make_slice("Transaction Amount Distribution", "dist_bar", ds_txns, {"groupby": ["transaction_amount_bin"], "metrics": [make_metric("COUNT(CASE WHEN is_fraud = 1 THEN 1 END)", "Fraud Txns"), make_metric("COUNT(CASE WHEN is_fraud = 0 THEN 1 END)", "Legit Txns")], "show_legend": True, "show_bar_value": False, "bar_stacked": True}, make_groupby_query(ds_txns.id, ["transaction_amount_bin"], [make_metric("COUNT(CASE WHEN is_fraud = 1 THEN 1 END)", "Fraud Txns"), make_metric("COUNT(CASE WHEN is_fraud = 0 THEN 1 END)", "Legit Txns")]))
        c8 = make_slice("Top Suspicious Transactions", "table", ds_top_susp, {"all_columns": ["transaction_id", "transaction_amount", "merchant_category", "merchant_location", "is_fraud", "event_time"], "page_size": 15}, make_table_columns_query(ds_top_susp.id, ["transaction_id", "transaction_amount", "merchant_category", "merchant_location", "is_fraud", "event_time"], 15))

        kpis = [k1, k2, k3, k4, k5, k6]
        charts = [c1, c2, c3, c4, c5, c6, c7, c8]
        
        dash = Dashboard(
            dashboard_title="Credit Card Fraud Detection — Real-Time Analytics",
            slug="credit-card-fraud-analytics",
            published=True,
            position_json=build_position_json(kpis, charts)
        )
        db.session.add(dash)
        dash.slices = kpis + charts

        # CSS and Metadata
        dash.css = """
        body, .dashboard, .dashboard-content { background-color: #111111 !important; color: #FFFFFF !important; font-family: 'Inter', sans-serif !important; }
        .dashboard-header { background-color: #111111 !important; border-bottom: none !important; padding: 16px 24px !important; }
        .dashboard-header .dashboard-header-title { color: #FFFFFF !important; font-weight: 800 !important; }
        .dashboard-component-chart-holder { background-color: #181818 !important; border: 1px solid #333333 !important; border-radius: 8px !important; }
        .table, .table th, .dashboard-component-tabs { background-color: #202020 !important; color: #FFFFFF !important; }
        .table th { border-bottom: 1px solid #333333 !important; color: #B8B8B8 !important; }
        .table td { border-bottom: 1px solid #333333 !important; color: #FFFFFF !important; }
        .chart-header .header-title, .header-title, .slice_name { color: #FFFFFF !important; }
        .text-muted, .ant-typography-secondary, .filter-title { color: #B8B8B8 !important; }
        .big_number_total .header-line { color: #FFFFFF !important; font-size: 2.5rem !important; }
        .big_number_total .subheader-line { color: #B8B8B8 !important; }
        """
        json_meta = {
            "label_colors": {
                "1": "#FF5A5F", "0": "#35C7C0",
                "Fraud Txns": "#FF5A5F", "Legit Txns": "#35C7C0",
                "Fraud Transactions": "#FF5A5F", "Total Transactions": "#35C7C0",
                "Fraud Rate %": "#FF5A5F", "Total Transaction Amount": "#35C7C0",
                "Fraud Amount": "#FF5A5F"
            },
            "color_scheme": "supersetColors"
        }
        dash.json_metadata = json.dumps(json_meta)
        
        db.session.commit()
        print("Successfully rebuilt dashboard with robust position_json layout!")

if __name__ == "__main__":
    run()
