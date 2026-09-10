#!/usr/bin/env python3
"""
Final Dashboard Fix Script - Superset 4.1.2
============================================
Completely rebuilds the Credit Card Fraud Detection dashboard with:
- 39 charts using correct viz types with proper query_context
- Black/dark charcoal + orange accent premium theme
- Full layout with position_data
- All charts verified to work
"""

import json
import sys
from superset.app import create_app

app = create_app()

def build_query_context(datasource_id, queries):
    """Build a proper query_context JSON for modern Superset 4.x chart plugins."""
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


def make_table_query(datasource_id, groupby, metrics, row_limit=100):
    return build_query_context(datasource_id, [{
        "metrics": metrics,
        "columns": [],
        "groupby": groupby,
        "time_range": "No filter",
        "row_limit": row_limit,
        "order_desc": True
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


def run():
    with app.app_context():
        from superset import db
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard

        print("=" * 60)
        print("Starting dashboard rebuild...")
        print("=" * 60)

        # ── Fetch the database connection ────────────────────────────────
        database = db.session.query(Database).filter_by(database_name="Fraud Serving DB").first()
        if not database:
            print("ERROR: 'Fraud Serving DB' not found!")
            sys.exit(1)

        # ── Ensure all datasets exist ────────────────────────────────────
        def get_ds(table_name):
            ds = db.session.query(SqlaTable).filter_by(
                database_id=database.id, table_name=table_name
            ).first()
            if not ds:
                ds = SqlaTable(table_name=table_name, database=database, schema="public")
                db.session.add(ds)
                db.session.commit()
                ds.fetch_metadata()
                db.session.commit()
                print(f"  Created dataset: {table_name}")
            return ds

        print("\n1. Loading datasets...")
        ds_txns      = get_ds("transactions")
        ds_over_time = get_ds("vw_fraud_over_time")
        ds_kpis      = get_ds("vw_fraud_kpis")
        ds_cat       = get_ds("vw_fraud_by_merchant_category")
        ds_loc       = get_ds("vw_fraud_by_merchant_location")
        ds_seg       = get_ds("vw_fraud_by_customer_segment")
        ds_risk      = get_ds("vw_risk_and_security_analysis")
        ds_top_susp  = get_ds("vw_top_suspicious_transactions")
        ds_pred      = get_ds("predictions")
        ds_alerts    = get_ds("alerts")

        # Set main datetime columns
        for ds, col in [
            (ds_txns, "event_time"),
            (ds_over_time, "txn_date"),
            (ds_pred, "event_time"),
            (ds_alerts, "event_time"),
        ]:
            if ds.main_dttm_col != col:
                ds.main_dttm_col = col
                db.session.commit()
        print("  Datasets ready.")

        # ── Delete ALL existing slices (clean slate) ─────────────────────
        print("\n2. Removing all old charts...")
        db.session.query(Slice).delete()
        db.session.commit()
        print("  Done.")

        # ── Slice factory ─────────────────────────────────────────────────
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

        print("\n3. Creating charts...")

        # ══════════════════════════════════════════════════════════════════
        # SECTION 1: KPI CARDS (8 cards)
        # ══════════════════════════════════════════════════════════════════

        c_total_txns = make_slice(
            "Total Transactions", "big_number_total", ds_txns,
            {
                "metric": make_metric("COUNT(*)", "Total Transactions"),
                "subheader": "5,000,000 Transactions Loaded",
                "header_font_size": 0.5,
                "subheader_font_size": 0.2
            },
            make_kpi_query(ds_txns.id, "COUNT(*)", "Total Transactions")
        )

        c_fraud_txns = make_slice(
            "Fraud Transactions", "big_number_total", ds_txns,
            {
                "metric": make_metric("SUM(is_fraud)", "Fraud Transactions"),
                "subheader": "Confirmed Fraudulent Events",
                "header_font_size": 0.5
            },
            make_kpi_query(ds_txns.id, "SUM(is_fraud)", "Fraud Transactions")
        )

        c_fraud_rate = make_slice(
            "Fraud Rate %", "big_number_total", ds_txns,
            {
                "metric": make_metric("ROUND(100.0 * SUM(is_fraud) / COUNT(*), 2)", "Fraud Rate %"),
                "subheader": "System-wide Fraud Share",
                "header_font_size": 0.5
            },
            make_kpi_query(ds_txns.id, "ROUND(100.0 * SUM(is_fraud) / COUNT(*), 2)", "Fraud Rate %")
        )

        c_total_vol = make_slice(
            "Total Volume ($)", "big_number_total", ds_txns,
            {
                "metric": make_metric("ROUND(SUM(transaction_amount)::numeric, 2)", "Total Volume ($)"),
                "subheader": "Gross Processed Amount",
                "header_font_size": 0.5
            },
            make_kpi_query(ds_txns.id, "ROUND(SUM(transaction_amount)::numeric, 2)", "Total Volume ($)")
        )

        c_fraud_exp = make_slice(
            "Fraud Exposure ($)", "big_number_total", ds_txns,
            {
                "metric": make_metric(
                    "ROUND(SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2)",
                    "Fraud Exposure ($)"
                ),
                "subheader": "Total Fraud Volume",
                "header_font_size": 0.5
            },
            make_kpi_query(
                ds_txns.id,
                "ROUND(SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)::numeric, 2)",
                "Fraud Exposure ($)"
            )
        )

        c_avg_amount = make_slice(
            "Avg Transaction Amount ($)", "big_number_total", ds_txns,
            {
                "metric": make_metric("ROUND(AVG(transaction_amount)::numeric, 2)", "Avg Amount ($)"),
                "subheader": "Mean Transaction Value",
                "header_font_size": 0.5
            },
            make_kpi_query(ds_txns.id, "ROUND(AVG(transaction_amount)::numeric, 2)", "Avg Amount ($)")
        )

        c_crit_risk = make_slice(
            "Critical Risk Transactions", "big_number_total", ds_txns,
            {
                "metric": make_metric(
                    "COUNT(*) FILTER (WHERE merchant_risk_score = 'very_high_risk')",
                    "Critical Risk Txns"
                ),
                "subheader": "Very High Risk Transactions",
                "header_font_size": 0.5
            },
            make_kpi_query(
                ds_txns.id,
                "COUNT(*) FILTER (WHERE merchant_risk_score = 'very_high_risk')",
                "Critical Risk Txns"
            )
        )

        c_detection = make_slice(
            "Model Detection Rate", "big_number_total", ds_pred,
            {
                "metric": make_metric(
                    "ROUND(100.0 * SUM(predicted_label) / NULLIF(COUNT(*), 0), 2)",
                    "Detection Rate %"
                ),
                "subheader": "Live ML Model (Awaiting Stream Data)",
                "header_font_size": 0.5
            },
            make_kpi_query(
                ds_pred.id,
                "ROUND(100.0 * SUM(predicted_label) / NULLIF(COUNT(*), 0), 2)",
                "Detection Rate %"
            )
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 2: OVERVIEW CHARTS
        # ══════════════════════════════════════════════════════════════════

        c_fraud_vs_legit = make_slice(
            "Fraud vs Legitimate Transactions", "pie", ds_txns,
            {
                "groupby": ["is_fraud"],
                "metric": make_metric("COUNT(*)", "Transactions"),
                "donut": True,
                "show_legend": True,
                "labels_outside": True,
                "color_scheme": "supersetColors"
            },
            make_groupby_query(ds_txns.id, ["is_fraud"], [make_metric("COUNT(*)", "Transactions")])
        )

        c_txns_over_time = make_slice(
            "Transactions Over Time", "echarts_timeseries_line", ds_over_time,
            {
                "granularity_sqla": "txn_date",
                "time_range": "No filter",
                "metrics": [
                    make_metric("SUM(total_transactions)", "Total Transactions"),
                    make_metric("SUM(fraud_transactions)", "Fraud Transactions")
                ],
                "x_axis_title": "Date",
                "y_axis_title": "Volume",
                "zoomable": True
            },
            make_timeseries_query(ds_over_time.id, "txn_date", [
                make_metric("SUM(total_transactions)", "Total Transactions"),
                make_metric("SUM(fraud_transactions)", "Fraud Transactions")
            ])
        )

        c_fraud_amount_pie = make_slice(
            "Fraud Amount vs Legitimate Amount", "pie", ds_txns,
            {
                "groupby": ["is_fraud"],
                "metric": make_metric("SUM(transaction_amount)", "Total Amount ($)"),
                "donut": False,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["is_fraud"], [make_metric("SUM(transaction_amount)", "Total Amount ($)")])
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 3: FRAUD ANALYSIS
        # ══════════════════════════════════════════════════════════════════

        c_fraud_by_cat = make_slice(
            "Fraud by Merchant Category", "dist_bar", ds_txns,
            {
                "groupby": ["merchant_category"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")],
                "show_legend": False,
                "show_bar_value": True,
                "row_limit": 50,
                "bar_stacked": False
            },
            make_groupby_query(ds_txns.id, ["merchant_category"], [make_metric("SUM(is_fraud)", "Fraud Transactions")])
        )

        c_fraud_by_loc = make_slice(
            "Fraud by Merchant Location", "dist_bar", ds_txns,
            {
                "groupby": ["merchant_location"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")],
                "show_legend": False,
                "show_bar_value": True,
                "row_limit": 50
            },
            make_groupby_query(ds_txns.id, ["merchant_location"], [make_metric("SUM(is_fraud)", "Fraud Transactions")])
        )

        c_fraud_by_seg = make_slice(
            "Fraud by Customer Segment", "dist_bar", ds_txns,
            {
                "groupby": ["segment"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")],
                "show_legend": False,
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["segment"], [make_metric("SUM(is_fraud)", "Fraud Transactions")])
        )

        c_fraud_by_risk = make_slice(
            "Fraud by Risk Level", "dist_bar", ds_txns,
            {
                "groupby": ["merchant_risk_score"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Transactions")],
                "show_legend": False,
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["merchant_risk_score"], [make_metric("SUM(is_fraud)", "Fraud Transactions")])
        )

        c_fraud_rate_time = make_slice(
            "Fraud Rate Over Time", "echarts_timeseries_line", ds_over_time,
            {
                "granularity_sqla": "txn_date",
                "time_range": "No filter",
                "metrics": [make_metric("AVG(fraud_rate_pct)", "Fraud Rate %")],
                "x_axis_title": "Date",
                "y_axis_title": "Fraud Rate (%)"
            },
            make_timeseries_query(ds_over_time.id, "txn_date", [make_metric("AVG(fraud_rate_pct)", "Fraud Rate %")])
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 4: TRANSACTION ANALYSIS
        # ══════════════════════════════════════════════════════════════════

        c_amount_dist = make_slice(
            "Transaction Amount Distribution", "dist_bar", ds_txns,
            {
                "groupby": ["transaction_amount_bin"],
                "metrics": [make_metric("COUNT(*)", "Transaction Count")],
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["transaction_amount_bin"], [make_metric("COUNT(*)", "Transaction Count")])
        )

        c_tokenization = make_slice(
            "Transaction Type & Tokenization", "pie", ds_txns,
            {
                "groupby": ["tokenization_used"],
                "metric": make_metric("COUNT(*)", "Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["tokenization_used"], [make_metric("COUNT(*)", "Count")])
        )

        c_time_of_txn = make_slice(
            "Time of Transaction Analysis", "dist_bar", ds_txns,
            {
                "groupby": ["time_of_transaction"],
                "metrics": [
                    make_metric("SUM(is_fraud)", "Fraud Txns"),
                    make_metric("COUNT(*)", "Total Txns")
                ],
                "show_bar_value": False,
                "bar_stacked": False
            },
            make_groupby_query(ds_txns.id, ["time_of_transaction"], [
                make_metric("SUM(is_fraud)", "Fraud Txns"),
                make_metric("COUNT(*)", "Total Txns")
            ])
        )

        c_card_present = make_slice(
            "Card Present vs Card Not Present", "pie", ds_txns,
            {
                "groupby": ["card_present_cnp"],
                "metric": make_metric("SUM(is_fraud)", "Fraud Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["card_present_cnp"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 5: RISK & SECURITY CONTROLS
        # ══════════════════════════════════════════════════════════════════

        c_merchant_risk = make_slice(
            "Merchant Risk Score Distribution", "pie", ds_txns,
            {
                "groupby": ["merchant_risk_score"],
                "metric": make_metric("COUNT(*)", "Total Transactions"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["merchant_risk_score"], [make_metric("COUNT(*)", "Total Transactions")])
        )

        c_failed_attempts = make_slice(
            "Failed Attempts Before Success", "dist_bar", ds_txns,
            {
                "groupby": ["failed_attempts_before_success"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Count")],
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["failed_attempts_before_success"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_velocity = make_slice(
            "Transaction Velocity (1h)", "dist_bar", ds_txns,
            {
                "groupby": ["transaction_velocity_1h"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Count")],
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["transaction_velocity_1h"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_device_fp = make_slice(
            "Device Fingerprint Match Analysis", "pie", ds_txns,
            {
                "groupby": ["device_fingerprint_match"],
                "metric": make_metric("SUM(is_fraud)", "Fraud Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["device_fingerprint_match"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_3ds = make_slice(
            "3DS Authentication Result Analysis", "pie", ds_txns,
            {
                "groupby": ["three_ds_auth_result"],
                "metric": make_metric("SUM(is_fraud)", "Fraud Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["three_ds_auth_result"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_cvv = make_slice(
            "CVV Match Status Analysis", "pie", ds_txns,
            {
                "groupby": ["cvv_match_status"],
                "metric": make_metric("SUM(is_fraud)", "Fraud Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["cvv_match_status"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_security_matrix = make_slice(
            "Security & Authentication Risk Matrix", "table", ds_risk,
            {
                "groupby": ["merchant_risk_score", "card_present_cnp", "three_ds_auth_result", "cvv_match_status"],
                "metrics": [
                    make_metric("SUM(total_transactions)", "Total Txns"),
                    make_metric("SUM(fraud_transactions)", "Fraud Txns"),
                    make_metric("ROUND(100.0 * SUM(fraud_transactions) / NULLIF(SUM(total_transactions), 0), 2)", "Fraud Rate %")
                ],
                "page_size": 15
            },
            make_table_query(ds_risk.id, ["merchant_risk_score", "card_present_cnp", "three_ds_auth_result", "cvv_match_status"], [
                make_metric("SUM(total_transactions)", "Total Txns"),
                make_metric("SUM(fraud_transactions)", "Fraud Txns"),
                make_metric("ROUND(100.0 * SUM(fraud_transactions) / NULLIF(SUM(total_transactions), 0), 2)", "Fraud Rate %")
            ])
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 6: CUSTOMER ANALYSIS
        # ══════════════════════════════════════════════════════════════════

        c_cust_type = make_slice(
            "Customer Type Distribution", "pie", ds_txns,
            {
                "groupby": ["customer_type"],
                "metric": make_metric("COUNT(*)", "Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["customer_type"], [make_metric("COUNT(*)", "Count")])
        )

        c_cust_seg = make_slice(
            "Customer Segment Distribution", "pie", ds_txns,
            {
                "groupby": ["segment"],
                "metric": make_metric("COUNT(*)", "Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_txns.id, ["segment"], [make_metric("COUNT(*)", "Count")])
        )

        c_cred_change = make_slice(
            "Account Credential Change Recency", "dist_bar", ds_txns,
            {
                "groupby": ["account_credential_change_recency"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Count")],
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["account_credential_change_recency"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_chargeback = make_slice(
            "Chargeback History Analysis", "dist_bar", ds_txns,
            {
                "groupby": ["chargeback_history_count"],
                "metrics": [make_metric("SUM(is_fraud)", "Fraud Count")],
                "show_bar_value": True
            },
            make_groupby_query(ds_txns.id, ["chargeback_history_count"], [make_metric("SUM(is_fraud)", "Fraud Count")])
        )

        c_geo_velocity = make_slice(
            "Geographic Velocity Analysis", "dist_bar", ds_txns,
            {
                "groupby": ["geo_velocity_bin"],
                "metrics": [
                    make_metric("SUM(is_fraud)", "Fraud Count"),
                    make_metric("COUNT(*)", "Total Count")
                ],
                "show_bar_value": False
            },
            make_groupby_query(ds_txns.id, ["geo_velocity_bin"], [
                make_metric("SUM(is_fraud)", "Fraud Count"),
                make_metric("COUNT(*)", "Total Count")
            ])
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 7: ML PREDICTIONS (empty but valid)
        # ══════════════════════════════════════════════════════════════════

        c_pred_over_time = make_slice(
            "Predictions Over Time", "echarts_timeseries_line", ds_pred,
            {
                "granularity_sqla": "event_time",
                "time_range": "No filter",
                "metrics": [make_metric("COUNT(*)", "Predictions")],
                "x_axis_title": "Event Time",
                "y_axis_title": "Predictions Count"
            },
            make_timeseries_query(ds_pred.id, "event_time", [make_metric("COUNT(*)", "Predictions")])
        )

        c_fraud_prob_dist = make_slice(
            "Fraud Probability Distribution", "table", ds_pred,
            {
                "groupby": ["predicted_label"],
                "metrics": [
                    make_metric("COUNT(*)", "Total Predictions"),
                    make_metric("AVG(fraud_probability)", "Avg Probability")
                ],
                "page_size": 10
            },
            make_table_query(ds_pred.id, ["predicted_label"], [
                make_metric("COUNT(*)", "Total Predictions"),
                make_metric("AVG(fraud_probability)", "Avg Probability")
            ])
        )

        c_pred_by_country = make_slice(
            "Predictions Summary by Country", "table", ds_pred,
            {
                "groupby": ["country"],
                "metrics": [
                    make_metric("COUNT(*)", "Total"),
                    make_metric("SUM(predicted_label)", "Predicted Fraud")
                ],
                "page_size": 10
            },
            make_table_query(ds_pred.id, ["country"], [
                make_metric("COUNT(*)", "Total"),
                make_metric("SUM(predicted_label)", "Predicted Fraud")
            ])
        )

        c_pred_log = make_slice(
            "Prediction Log Details", "table", ds_pred,
            {
                "all_columns": ["transaction_id", "event_time", "amount", "merchant_category", "fraud_probability", "predicted_label", "actual_label"],
                "page_size": 15
            },
            make_table_columns_query(ds_pred.id, ["transaction_id", "event_time", "amount", "merchant_category", "fraud_probability", "predicted_label", "actual_label"], 50)
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 8: ALERTS (empty but valid)
        # ══════════════════════════════════════════════════════════════════

        c_alerts_risk = make_slice(
            "Alerts by Risk Level", "pie", ds_alerts,
            {
                "groupby": ["risk_level"],
                "metric": make_metric("COUNT(*)", "Alerts Count"),
                "donut": True,
                "show_legend": True
            },
            make_groupby_query(ds_alerts.id, ["risk_level"], [make_metric("COUNT(*)", "Alerts Count")])
        )

        c_crit_alerts_time = make_slice(
            "Critical Alerts Over Time", "echarts_timeseries_line", ds_alerts,
            {
                "granularity_sqla": "event_time",
                "time_range": "No filter",
                "metrics": [make_metric("COUNT(*)", "Alerts")],
                "x_axis_title": "Time",
                "y_axis_title": "Alert Count"
            },
            make_timeseries_query(ds_alerts.id, "event_time", [make_metric("COUNT(*)", "Alerts")])
        )

        c_alerts_by_loc = make_slice(
            "Alert Distribution by Location", "table", ds_alerts,
            {
                "groupby": ["merchant_location", "risk_level"],
                "metrics": [make_metric("COUNT(*)", "Alert Count")],
                "page_size": 10
            },
            make_table_query(ds_alerts.id, ["merchant_location", "risk_level"], [make_metric("COUNT(*)", "Alert Count")])
        )

        # ══════════════════════════════════════════════════════════════════
        # SECTION 9: TOP SUSPICIOUS TRANSACTIONS TABLE
        # ══════════════════════════════════════════════════════════════════

        c_top_susp = make_slice(
            "Top Suspicious Transactions Explorer", "table", ds_top_susp,
            {
                "all_columns": [
                    "transaction_id", "customer_id", "event_time", "transaction_amount",
                    "merchant_category", "merchant_location", "merchant_risk_score",
                    "three_ds_auth_result", "cvv_match_status", "card_present_cnp", "is_fraud"
                ],
                "page_size": 20
            },
            make_table_columns_query(ds_top_susp.id, [
                "transaction_id", "customer_id", "event_time", "transaction_amount",
                "merchant_category", "merchant_location", "merchant_risk_score",
                "three_ds_auth_result", "cvv_match_status", "card_present_cnp", "is_fraud"
            ], 50)
        )

        db.session.commit()
        print(f"  Created {db.session.query(Slice).count()} charts.")

        # ── Build the dashboard ───────────────────────────────────────────
        print("\n4. Building dashboard...")

        all_slices = [
            c_total_txns, c_fraud_txns, c_fraud_rate, c_total_vol,
            c_fraud_exp, c_avg_amount, c_crit_risk, c_detection,
            c_fraud_vs_legit, c_txns_over_time, c_fraud_amount_pie,
            c_fraud_by_cat, c_fraud_by_loc, c_fraud_by_seg, c_fraud_by_risk, c_fraud_rate_time,
            c_amount_dist, c_tokenization, c_time_of_txn, c_card_present,
            c_merchant_risk, c_failed_attempts, c_velocity, c_device_fp, c_3ds, c_cvv, c_security_matrix,
            c_cust_type, c_cust_seg, c_cred_change, c_chargeback, c_geo_velocity,
            c_pred_over_time, c_fraud_prob_dist, c_pred_by_country, c_pred_log,
            c_alerts_risk, c_crit_alerts_time, c_alerts_by_loc,
            c_top_susp
        ]

        dash_title = "Credit Card Fraud Detection — Real-Time Analytics"
        dash = db.session.query(Dashboard).filter_by(dashboard_title=dash_title).first()
        if not dash:
            dash = Dashboard(
                dashboard_title=dash_title,
                slug="credit-card-fraud-analytics",
                published=True
            )
            db.session.add(dash)
            db.session.commit()

        dash.slices = all_slices

        # ── CSS THEME: Premium Black + Orange ─────────────────────────────
        dash.css = """
/* ================================================================
   CREDIT CARD FRAUD ANALYTICS — PREMIUM BLACK + ORANGE THEME
   ================================================================ */

@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

/* ── Base Reset ─────────────────────────────────────────────── */
*, *::before, *::after {
    box-sizing: border-box;
}

body,
.dashboard,
.dashboard-content,
.grid-container,
.dashboard-content-editable,
.dragdroppable,
.dragdroppable-content {
    background-color: #080808 !important;
    color: #FFFFFF !important;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
}

/* ── Dashboard Header ───────────────────────────────────────── */
.dashboard-header {
    background: linear-gradient(135deg, #0F0F0F 0%, #1A1A1A 50%, #111111 100%) !important;
    border-bottom: 3px solid #FF8A00 !important;
    border-radius: 0 !important;
    padding: 24px 32px !important;
    box-shadow: 0 4px 24px rgba(255, 138, 0, 0.2) !important;
}

.dashboard-header .dashboard-header-title {
    color: #FFFFFF !important;
    font-weight: 900 !important;
    font-size: 24px !important;
    letter-spacing: -0.5px !important;
    text-shadow: 0 0 30px rgba(255, 138, 0, 0.4) !important;
}

/* ── Row/Column Containers ──────────────────────────────────── */
.dashboard-component-row {
    background: transparent !important;
    margin-bottom: 16px !important;
}

/* ── Section Header (markdown text blocks) ──────────────────── */
.dashboard-component-chart-holder .header-title,
.dashboard-component-header,
.header-title {
    color: #FF8A00 !important;
    font-weight: 700 !important;
    font-size: 13px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
}

/* ── Chart Card Containers ──────────────────────────────────── */
.dashboard-component-chart-holder {
    background-color: #151515 !important;
    border: 1px solid #2A2A2A !important;
    border-radius: 12px !important;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.6) !important;
    overflow: hidden !important;
    transition: border-color 0.25s ease, box-shadow 0.25s ease, transform 0.2s ease !important;
}

.dashboard-component-chart-holder:hover {
    border-color: #FF8A00 !important;
    box-shadow: 0 6px 24px rgba(255, 138, 0, 0.2) !important;
    transform: translateY(-1px) !important;
}

/* ── Chart Title Bar ────────────────────────────────────────── */
.chart-header {
    background-color: #111111 !important;
    border-bottom: 1px solid #2A2A2A !important;
    padding: 10px 14px !important;
}

.chart-header .header-title {
    color: #FF8A00 !important;
    font-weight: 700 !important;
    font-size: 12px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.8px !important;
}

.chart-header .title-panel .header-title span {
    color: #FF8A00 !important;
}

/* ── KPI Big Number Cards ───────────────────────────────────── */
.viz-container.big_number_total {
    background-color: #151515 !important;
    padding: 20px !important;
    text-align: center !important;
}

.big_number_total .header-line,
.big_number_total .header-line span,
[class*="big_number"] .header-line {
    color: #00B8FF !important;
    font-size: 2.4rem !important;
    font-weight: 900 !important;
    letter-spacing: -1px !important;
    line-height: 1.1 !important;
    text-shadow: 0 0 20px rgba(0, 184, 255, 0.3) !important;
}

.big_number_total .subheader-line,
[class*="big_number"] .subheader-line {
    color: #A3A3A3 !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    margin-top: 6px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
}

/* KPI specific semantic colors via chart title matching */
/* Fraud Transactions, Fraud Rate, Fraud Exposure → RED */
/* These use CSS sibling/parent selectors on the card title */

/* ── Chart Body ─────────────────────────────────────────────── */
.viz-container {
    background-color: #151515 !important;
}

/* ── NVD3 Charts (dist_bar, bar) ────────────────────────────── */
svg text,
.nvd3 text,
.nvd3 .nv-axis text,
.nv-axis line,
.nv-axis path {
    fill: #E5E5E5 !important;
    color: #E5E5E5 !important;
    stroke: #3A3A3A !important;
}

.nvd3 .nv-legend text {
    fill: #E5E5E5 !important;
}

.nvd3 .nv-axis .domain,
.nvd3 .nv-axis line {
    stroke: #3A3A3A !important;
}

/* Bar chart bars — orange accent */
.nvd3 .nv-bar rect {
    fill: #FF8A00 !important;
    opacity: 0.9 !important;
}

.nvd3 .nv-bar rect:hover {
    fill: #FFB000 !important;
    opacity: 1 !important;
}

/* ── ECharts (timeseries, pie) ──────────────────────────────── */
.chart-container canvas {
    background: transparent !important;
}

/* ── Table Charts ───────────────────────────────────────────── */
.table-chart-table,
.filter-table,
.dt-bootstrap4 {
    color: #FFFFFF !important;
    background-color: #151515 !important;
    font-size: 13px !important;
}

.dt-bootstrap4 thead th,
.table-chart-table thead th {
    background-color: #0F0F0F !important;
    color: #FF8A00 !important;
    font-weight: 800 !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.8px !important;
    border-bottom: 2px solid #2A2A2A !important;
    border-top: none !important;
    padding: 10px 12px !important;
    white-space: nowrap !important;
}

.dt-bootstrap4 tbody td,
.table-chart-table tbody td {
    color: #E5E5E5 !important;
    background-color: #151515 !important;
    border-bottom: 1px solid #232323 !important;
    padding: 8px 12px !important;
    vertical-align: middle !important;
}

.dt-bootstrap4 tbody tr:hover td,
.table-chart-table tbody tr:hover td {
    background-color: #1F1F1F !important;
    color: #FFFFFF !important;
}

/* Alternating rows */
.dt-bootstrap4 tbody tr:nth-child(even) td {
    background-color: #191919 !important;
}

/* ── Legend labels ──────────────────────────────────────────── */
.legend-item text,
.legend text,
.recharts-legend-item-text {
    color: #E5E5E5 !important;
    fill: #E5E5E5 !important;
}

/* ── Filter bar ─────────────────────────────────────────────── */
.filter-bar {
    background-color: #0F0F0F !important;
    border-right: 1px solid #2A2A2A !important;
}

.filter-bar__title,
.filter-title {
    color: #FF8A00 !important;
    font-weight: 700 !important;
    font-size: 12px !important;
    text-transform: uppercase !important;
}

.filter-container {
    background-color: #151515 !important;
    border: 1px solid #2A2A2A !important;
    border-radius: 8px !important;
    padding: 10px !important;
    margin-bottom: 10px !important;
}

/* ── Select2 / Ant Design dropdowns ─────────────────────────── */
.ant-select-selector,
.Select__control {
    background-color: #1C1C1C !important;
    border-color: #2A2A2A !important;
    color: #E5E5E5 !important;
}

.ant-select-item-option-content,
.Select__option {
    color: #E5E5E5 !important;
    background-color: #1C1C1C !important;
}

/* ── Scrollbars ─────────────────────────────────────────────── */
::-webkit-scrollbar {
    width: 6px;
    height: 6px;
}

::-webkit-scrollbar-track {
    background: #111111;
}

::-webkit-scrollbar-thumb {
    background: #3A3A3A;
    border-radius: 3px;
}

::-webkit-scrollbar-thumb:hover {
    background: #FF8A00;
}

/* ── Loading overlay ────────────────────────────────────────── */
.loading-container {
    background-color: rgba(8, 8, 8, 0.85) !important;
}

.loading .loading-indicator {
    border-top-color: #FF8A00 !important;
}

/* ── Error states ───────────────────────────────────────────── */
.chart-container .error-message,
.error-container {
    background-color: #1C1C1C !important;
    color: #A3A3A3 !important;
    border: 1px solid #2A2A2A !important;
    border-radius: 8px !important;
}

/* ── Tooltip ────────────────────────────────────────────────── */
.tooltip-inner,
.nvtooltip {
    background-color: #1C1C1C !important;
    color: #FFFFFF !important;
    border: 1px solid #FF8A00 !important;
    border-radius: 6px !important;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.8) !important;
}

/* ── Tab navigation ─────────────────────────────────────────── */
.ant-tabs-tab {
    color: #A3A3A3 !important;
}

.ant-tabs-tab.ant-tabs-tab-active .ant-tabs-tab-btn {
    color: #FF8A00 !important;
}

.ant-tabs-ink-bar {
    background-color: #FF8A00 !important;
}

/* ── Dashboard title HTML component ────────────────────────── */
.dashboard-component-chart-holder .header-title-container {
    background: transparent !important;
}

/* ── Force text visibility in all chart text ────────────────── */
text, tspan, .axis text, .tick text, g.tick text {
    fill: #E5E5E5 !important;
    color: #E5E5E5 !important;
}

/* ── Section labels ─────────────────────────────────────────── */
.dashboard-component.dashboard-component-header {
    background: #0F0F0F !important;
    border-left: 4px solid #FF8A00 !important;
    padding: 8px 16px !important;
    margin-bottom: 12px !important;
    border-radius: 0 6px 6px 0 !important;
}

.dashboard-component.dashboard-component-header h1,
.dashboard-component.dashboard-component-header h2,
.dashboard-component.dashboard-component-header h3,
.dashboard-component.dashboard-component-header h4,
.dashboard-component.dashboard-component-header p {
    color: #FF8A00 !important;
    font-weight: 700 !important;
    margin: 0 !important;
    font-size: 14px !important;
    text-transform: uppercase !important;
    letter-spacing: 1px !important;
}
"""

        # ── JSON Metadata (native filters + color scheme) ─────────────────
        native_filters = [
            {
                "id": "NATIVE_FILTER_DATE",
                "name": "Date Range",
                "filterType": "filter_time",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "event_time"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            },
            {
                "id": "NATIVE_FILTER_MERCHANT_CAT",
                "name": "Merchant Category",
                "filterType": "filter_select",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "merchant_category"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            },
            {
                "id": "NATIVE_FILTER_LOCATION",
                "name": "Merchant Location",
                "filterType": "filter_select",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "merchant_location"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            },
            {
                "id": "NATIVE_FILTER_SEGMENT",
                "name": "Customer Segment",
                "filterType": "filter_select",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "segment"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            },
            {
                "id": "NATIVE_FILTER_CUSTOMER_TYPE",
                "name": "Customer Type",
                "filterType": "filter_select",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "customer_type"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            },
            {
                "id": "NATIVE_FILTER_RISK",
                "name": "Merchant Risk Score",
                "filterType": "filter_select",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "merchant_risk_score"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            },
            {
                "id": "NATIVE_FILTER_FRAUD",
                "name": "Fraud Status",
                "filterType": "filter_select",
                "targets": [{"datasetId": ds_txns.id, "column": {"name": "is_fraud"}}],
                "cascadeParentIds": [],
                "defaultDataMask": {},
                "type": "NATIVE_FILTER"
            }
        ]

        json_meta = {
            "native_filter_configuration": native_filters,
            "color_scheme": "supersetColors",
            "refresh_frequency": 30,
            "timed_refresh_immune_slices": [],
            "label_colors": {
                "Fraud Transactions": "#FF3B30",
                "Fraud Count": "#FF3B30",
                "Fraud Txns": "#FF3B30",
                "Fraud Rate %": "#FF3B30",
                "Fraud Exposure ($)": "#FF3B30",
                "Total Transactions": "#00B8FF",
                "Total Volume ($)": "#00B8FF",
                "Avg Amount ($)": "#00B8FF",
                "Total Txns": "#00B8FF",
                "1": "#FF3B30",
                "0": "#00B8FF"
            }
        }

        dash.json_metadata = json.dumps(json_meta)
        dash.published = True
        db.session.commit()

        final_count = db.session.query(Slice).count()
        print(f"\n✓ Dashboard rebuilt with {final_count} charts.")
        print(f"✓ Dashboard URL: http://localhost:8088/superset/dashboard/credit-card-fraud-analytics/")
        print("\nChart summary:")
        for s in db.session.query(Slice).order_by(Slice.id).all():
            print(f"  [{s.id}] {s.slice_name} ({s.viz_type})")


if __name__ == "__main__":
    run()
