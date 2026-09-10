#!/usr/bin/env python3
"""
Comprehensive Apache Superset Analytics Layer Initializer
---------------------------------------------------------
Automates dataset registration, chart creation (39 charts across 7 sections),
native filter layout, visual theme, and high-contrast Black + Orange color scheme for:
"Credit Card Fraud Detection — Real-Time Analytics"
"""

import json
import os
import sys

from superset.app import create_app

app = create_app()

def setup_superset_dashboard():
    with app.app_context():
        try:
            from superset import db
            from superset.models.core import Database
            from superset.connectors.sqla.models import SqlaTable
            from superset.models.slice import Slice
            from superset.models.dashboard import Dashboard
        except ImportError as e:
            print(f"Error importing Superset modules: {e}")
            sys.exit(1)

        print("Fetching Fraud Serving DB connection...")
        database = db.session.query(Database).filter_by(database_name="Fraud Serving DB").first()
        if not database:
            print("Error: 'Fraud Serving DB' connection not found in Superset.")
            sys.exit(1)

        def get_or_create_dataset(table_name):
            ds = db.session.query(SqlaTable).filter_by(database_id=database.id, table_name=table_name).first()
            if not ds:
                ds = SqlaTable(table_name=table_name, database=database, schema="public")
                db.session.add(ds)
                db.session.commit()
                print(f"Created dataset: {table_name}")
            else:
                print(f"Dataset exists: {table_name}")
            return ds

        # 1. Datasets
        ds_txns = get_or_create_dataset("transactions")
        ds_kpis = get_or_create_dataset("vw_fraud_kpis")
        ds_over_time = get_or_create_dataset("vw_fraud_over_time")
        ds_cat = get_or_create_dataset("vw_fraud_by_merchant_category")
        ds_loc = get_or_create_dataset("vw_fraud_by_merchant_location")
        ds_seg = get_or_create_dataset("vw_fraud_by_customer_segment")
        ds_risk = get_or_create_dataset("vw_risk_and_security_analysis")
        ds_top_susp = get_or_create_dataset("vw_top_suspicious_transactions")
        ds_predictions = get_or_create_dataset("predictions")
        ds_alerts = get_or_create_dataset("alerts")

        # Fetch column metadata
        for ds in [ds_txns, ds_kpis, ds_over_time, ds_cat, ds_loc, ds_seg, ds_risk, ds_top_susp, ds_predictions, ds_alerts]:
            try:
                ds.fetch_metadata()
                db.session.commit()
            except Exception as e:
                print(f"Metadata fetch notice for {ds.table_name}: {e}")

        # 2. Slice Helper
        def get_or_create_slice(slice_name, viz_type, datasource, params):
            slc = db.session.query(Slice).filter_by(slice_name=slice_name).first()
            params["viz_type"] = viz_type
            params["datasource"] = f"{datasource.id}__table"
            if not slc:
                slc = Slice(
                    slice_name=slice_name,
                    viz_type=viz_type,
                    datasource_type="table",
                    datasource_id=datasource.id,
                    params=json.dumps(params)
                )
                db.session.add(slc)
                db.session.commit()
                print(f"Created chart: {slice_name}")
            else:
                slc.datasource_id = datasource.id
                slc.viz_type = viz_type
                slc.params = json.dumps(params)
                db.session.commit()
                print(f"Updated chart: {slice_name}")
            return slc

        # ------------------------------------------------------------
        # SECTION 1: HEADER & KPI CARDS (8 KPIs)
        # ------------------------------------------------------------
        c1 = get_or_create_slice(
            "Total Transactions KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Total Transactions"},
                "subheader": "5,000,000 Real Transactions Loaded",
                "header_font_size": 0.45
            }
        )

        c2 = get_or_create_slice(
            "Fraud Transactions KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Transactions"},
                "subheader": "Confirmed Fraudulent Events",
                "header_font_size": 0.45
            }
        )

        c3 = get_or_create_slice(
            "Fraud Rate % KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "ROUND(100.0 * SUM(is_fraud) / COUNT(*), 2)", "label": "Fraud Rate %"},
                "subheader": "System-wide Fraud Share",
                "header_font_size": 0.45
            }
        )

        c4 = get_or_create_slice(
            "Total Volume ($) KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(transaction_amount)", "label": "Total Volume ($)"},
                "subheader": "Gross Processed Amount",
                "header_font_size": 0.45
            }
        )

        c5 = get_or_create_slice(
            "Fraud Exposure ($) KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(CASE WHEN is_fraud = 1 THEN transaction_amount ELSE 0 END)", "label": "Fraud Exposure ($)"},
                "subheader": "Total Fraud Volume",
                "header_font_size": 0.45
            }
        )

        c6 = get_or_create_slice(
            "Average Transaction Amount KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(transaction_amount)::numeric, 2)", "label": "Avg Transaction Amount ($)"},
                "subheader": "Mean Transaction Value",
                "header_font_size": 0.45
            }
        )

        c7 = get_or_create_slice(
            "Critical Risk Transactions KPI",
            "big_number_total",
            ds_txns,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*) FILTER (WHERE merchant_risk_score = 'very_high_risk')", "label": "Critical Risk Txns"},
                "subheader": "Very High Risk Category Count",
                "header_font_size": 0.45
            }
        )

        c8 = get_or_create_slice(
            "Model Detection Rate KPI",
            "big_number_total",
            ds_predictions,
            {
                "metric": {"expressionType": "SQL", "sqlExpression": "ROUND(100.0 * SUM(predicted_label) / NULLIF(COUNT(*), 0), 2)", "label": "Detection Rate %"},
                "subheader": "Real-time ML Model Detection Rate",
                "header_font_size": 0.45
            }
        )

        # ------------------------------------------------------------
        # SECTION 2: OVERVIEW
        # ------------------------------------------------------------
        c9 = get_or_create_slice(
            "Fraud vs Legitimate Transactions",
            "pie",
            ds_txns,
            {
                "groupby": ["is_fraud"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Transactions"},
                "donut": True,
                "show_legend": True,
                "labels_outside": True
            }
        )

        c10 = get_or_create_slice(
            "Transactions Over Time",
            "echarts_timeseries_line",
            ds_over_time,
            {
                "granularity_sqla": "txn_date",
                "time_range": "No filter",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "SUM(total_transactions)", "label": "Total Transactions"},
                    {"expressionType": "SQL", "sqlExpression": "SUM(fraud_transactions)", "label": "Fraud Transactions"}
                ],
                "x_axis_title": "Date",
                "y_axis_title": "Volume"
            }
        )

        c11 = get_or_create_slice(
            "Fraud Amount vs Legitimate Amount",
            "pie",
            ds_txns,
            {
                "groupby": ["is_fraud"],
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(transaction_amount)", "label": "Total Amount ($)"},
                "donut": False,
                "show_legend": True
            }
        )

        # ------------------------------------------------------------
        # SECTION 3: FRAUD ANALYSIS
        # ------------------------------------------------------------
        c12 = get_or_create_slice(
            "Fraud by Merchant Category",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["merchant_category"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Transactions"}],
                "show_legend": True,
                "show_bar_value": True,
                "row_limit": 50
            }
        )

        c13 = get_or_create_slice(
            "Fraud by Merchant Location",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["merchant_location"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Transactions"}],
                "show_legend": True,
                "show_bar_value": True,
                "row_limit": 50
            }
        )

        c14 = get_or_create_slice(
            "Fraud by Customer Segment",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["segment"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Transactions"}],
                "show_legend": True,
                "show_bar_value": True,
                "row_limit": 50
            }
        )

        c15 = get_or_create_slice(
            "Fraud by Risk Level",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["merchant_risk_score"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Transactions"}],
                "show_legend": True,
                "show_bar_value": True
            }
        )

        # ------------------------------------------------------------
        # SECTION 4: TRANSACTION ANALYSIS
        # ------------------------------------------------------------
        c16 = get_or_create_slice(
            "Transaction Amount Distribution",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["transaction_amount_bin"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Transaction Count"}],
                "show_bar_value": True
            }
        )

        c17 = get_or_create_slice(
            "Fraud Rate Over Time",
            "echarts_timeseries_line",
            ds_over_time,
            {
                "granularity_sqla": "txn_date",
                "time_range": "No filter",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "AVG(fraud_rate_pct)", "label": "Fraud Rate %"}
                ],
                "x_axis_title": "Date",
                "y_axis_title": "Fraud Rate %"
            }
        )

        c18 = get_or_create_slice(
            "Transaction Type & Tokenization Distribution",
            "pie",
            ds_txns,
            {
                "groupby": ["tokenization_used"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Count"},
                "donut": True
            }
        )

        c19 = get_or_create_slice(
            "Time of Transaction Analysis",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["time_of_transaction"],
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Txns"},
                    {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Total Txns"}
                ],
                "show_bar_value": True
            }
        )

        c20 = get_or_create_slice(
            "Card Present vs Card Not Present",
            "pie",
            ds_txns,
            {
                "groupby": ["card_present_cnp"],
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"},
                "donut": True
            }
        )

        # ------------------------------------------------------------
        # SECTION 5: RISK & SECURITY CONTROLS
        # ------------------------------------------------------------
        c21 = get_or_create_slice(
            "Merchant Risk Score Distribution",
            "pie",
            ds_txns,
            {
                "groupby": ["merchant_risk_score"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Total Transactions"},
                "donut": True
            }
        )

        c22 = get_or_create_slice(
            "Failed Attempts Before Success",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["failed_attempts_before_success"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"}],
                "show_bar_value": True
            }
        )

        c23 = get_or_create_slice(
            "Transaction Velocity (1h)",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["transaction_velocity_1h"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"}],
                "show_bar_value": True
            }
        )

        c24 = get_or_create_slice(
            "Device Fingerprint Match Analysis",
            "pie",
            ds_txns,
            {
                "groupby": ["device_fingerprint_match"],
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"},
                "donut": True
            }
        )

        c25 = get_or_create_slice(
            "3DS Authentication Result Analysis",
            "pie",
            ds_txns,
            {
                "groupby": ["three_ds_auth_result"],
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"},
                "donut": True
            }
        )

        c26 = get_or_create_slice(
            "CVV Match Status Analysis",
            "pie",
            ds_txns,
            {
                "groupby": ["cvv_match_status"],
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"},
                "donut": True
            }
        )

        # ------------------------------------------------------------
        # SECTION 6: CUSTOMER ANALYSIS
        # ------------------------------------------------------------
        c27 = get_or_create_slice(
            "Customer Type Distribution",
            "pie",
            ds_txns,
            {
                "groupby": ["customer_type"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Count"},
                "donut": True
            }
        )

        c28 = get_or_create_slice(
            "Customer Segment Distribution",
            "pie",
            ds_txns,
            {
                "groupby": ["segment"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Count"},
                "donut": True
            }
        )

        c29 = get_or_create_slice(
            "Account Credential Change Recency",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["account_credential_change_recency"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"}],
                "show_bar_value": True
            }
        )

        c30 = get_or_create_slice(
            "Chargeback History Analysis",
            "dist_bar",
            ds_txns,
            {
                "groupby": ["chargeback_history_count"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(is_fraud)", "label": "Fraud Count"}],
                "show_bar_value": True
            }
        )

        # ------------------------------------------------------------
        # SECTION 7: MACHINE LEARNING & PREDICTIONS
        # ------------------------------------------------------------
        c31 = get_or_create_slice(
            "Predictions Over Time",
            "echarts_timeseries_line",
            ds_predictions,
            {
                "granularity_sqla": "event_time",
                "time_range": "No filter",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Predictions"}],
                "x_axis_title": "Event Time",
                "y_axis_title": "Predictions Count"
            }
        )

        c32 = get_or_create_slice(
            "Fraud Probability Distribution",
            "table",
            ds_predictions,
            {
                "groupby": ["predicted_label"],
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Total Predictions"},
                    {"expressionType": "SQL", "sqlExpression": "AVG(fraud_probability)", "label": "Avg Probability"}
                ],
                "page_size": 10
            }
        )

        c33 = get_or_create_slice(
            "Predictions Summary by Country",
            "table",
            ds_predictions,
            {
                "groupby": ["country"],
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Total"},
                    {"expressionType": "SQL", "sqlExpression": "SUM(predicted_label)", "label": "Predicted Fraud"}
                ],
                "page_size": 10
            }
        )

        c34 = get_or_create_slice(
            "Prediction Log Details",
            "table",
            ds_predictions,
            {
                "all_columns": ["transaction_id", "event_time", "amount", "merchant_category", "fraud_probability", "predicted_label", "actual_label"],
                "page_size": 15
            }
        )

        # ------------------------------------------------------------
        # SECTION 8: ALERTS & TOP SUSPICIOUS TRANSACTIONS
        # ------------------------------------------------------------
        c35 = get_or_create_slice(
            "Alerts by Risk Level",
            "pie",
            ds_alerts,
            {
                "groupby": ["risk_level"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Alerts Count"},
                "donut": True
            }
        )

        c36 = get_or_create_slice(
            "Critical Alerts Over Time",
            "echarts_timeseries_line",
            ds_alerts,
            {
                "granularity_sqla": "event_time",
                "time_range": "No filter",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Alerts"}],
                "x_axis_title": "Time",
                "y_axis_title": "Alert Count"
            }
        )

        c37 = get_or_create_slice(
            "Alert Distribution by Merchant Location",
            "table",
            ds_alerts,
            {
                "groupby": ["merchant_location", "risk_level"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Alert Count"}],
                "page_size": 10
            }
        )

        c38 = get_or_create_slice(
            "Security & Authentication Risk Matrix",
            "table",
            ds_risk,
            {
                "groupby": ["merchant_risk_score", "card_present_cnp", "three_ds_auth_result", "cvv_match_status"],
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "SUM(total_transactions)", "label": "Total Txns"},
                    {"expressionType": "SQL", "sqlExpression": "SUM(fraud_transactions)", "label": "Fraud Txns"},
                    {"expressionType": "SQL", "sqlExpression": "ROUND(100.0 * SUM(fraud_transactions) / NULLIF(SUM(total_transactions), 0), 2)", "label": "Fraud Rate %"}
                ],
                "page_size": 15
            }
        )

        c39 = get_or_create_slice(
            "Top Suspicious Transactions Explorer",
            "table",
            ds_top_susp,
            {
                "all_columns": [
                    "transaction_id", "customer_id", "event_time", "transaction_amount",
                    "merchant_category", "merchant_location", "merchant_risk_score",
                    "three_ds_auth_result", "cvv_match_status", "card_present_cnp", "is_fraud"
                ],
                "page_size": 20
            }
        )

        # 3. Attach Slices to Dashboard
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

        clean_slices = [
            c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11,
            c12, c13, c14, c15,
            c16, c17, c18, c19, c20,
            c21, c22, c23, c24, c25, c26,
            c27, c28, c29, c30,
            c31, c32, c33, c34,
            c35, c36, c37, c38, c39
        ]

        dash.slices = clean_slices

        # High-Contrast BLACK + ORANGE SOC Fraud Monitoring Theme
        dash.css = """
        /* Premium Black Background */
        body, .dashboard, .dashboard-content, .grid-container {
            background-color: #080808 !important;
            color: #FFFFFF !important;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif !important;
        }

        /* Main Top Header */
        .dashboard-header {
            background: linear-gradient(135deg, #111111 0%, #171717 100%) !important;
            border-bottom: 3px solid #FF8A00 !important;
            padding: 20px 24px !important;
            border-radius: 12px !important;
            margin-bottom: 24px !important;
            box-shadow: 0 6px 24px rgba(255, 138, 0, 0.15) !important;
        }
        .dashboard-header .header-title {
            color: #FFFFFF !important;
            font-weight: 900 !important;
            font-size: 26px !important;
            letter-spacing: -0.5px !important;
        }

        /* Component Card Holders */
        .dashboard-component-chart-holder {
            background-color: #171717 !important;
            border: 1px solid #292929 !important;
            border-radius: 10px !important;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.7) !important;
            margin-bottom: 16px !important;
            transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
        }
        .dashboard-component-chart-holder:hover {
            border-color: #FF8A00 !important;
            box-shadow: 0 6px 20px rgba(255, 138, 0, 0.25) !important;
        }

        /* Chart Header Titles */
        .chart-header .header-title {
            color: #FF8A00 !important;
            font-weight: 700 !important;
            font-size: 15px !important;
        }

        /* KPI Card Styling & High-Contrast Colors */
        .big_number_total .header-line {
            font-weight: 900 !important;
            font-size: 2.2rem !important;
            color: #00B8FF !important;
        }
        .big_number_total .subheader-line {
            color: #9CA3AF !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
        }

        /* Data Tables */
        .table {
            color: #FFFFFF !important;
            background-color: #171717 !important;
        }
        .table th {
            background-color: #111111 !important;
            color: #FF8A00 !important;
            font-weight: 800 !important;
            border-bottom: 2px solid #292929 !important;
            font-size: 12px !important;
            text-transform: uppercase !important;
        }
        .table td {
            border-bottom: 1px solid #292929 !important;
            font-size: 13px !important;
            color: #D1D5DB !important;
        }
        """

        # Native Global Filters (Black/Orange active styling)
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
            "refresh_frequency": 30
        }

        dash.json_metadata = json.dumps(json_meta)
        dash.published = True
        db.session.commit()
        print("All 39 charts, native filters, and Black + Orange SOC theme dashboard successfully deployed!")

if __name__ == "__main__":
    setup_superset_dashboard()
