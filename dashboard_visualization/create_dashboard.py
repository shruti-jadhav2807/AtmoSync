"""Create and verify the AtmoSync Smart Container Monitoring Dashboard via Superset REST API."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "http://127.0.0.1:8088"
DASHBOARD_TITLE = "AtmoSync – Smart Container Monitoring Dashboard"
DASHBOARD_SLUG = "atmosync-smart-container-monitoring"


class SupersetClient:
    """Minimal authenticated Superset 4.1.1 REST API client."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
        login_resp = self._raw_request(
            "POST",
            "/api/v1/security/login",
            {"username": username, "password": password, "provider": "db", "refresh": True},
        )
        self.access_token = login_resp["access_token"]
        csrf_resp = self._raw_request("GET", "/api/v1/security/csrf_token/", headers=self._auth_headers())
        self.csrf_token = csrf_resp["result"]

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Referer": self.base_url,
        }

    def _raw_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers or {"Content-Type": "application/json"},
            method=method,
        )
        try:
            with self.opener.open(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} on {method} {path}: {detail}") from exc

    def get(self, path: str) -> dict[str, Any]:
        return self._raw_request("GET", path, headers=self._auth_headers())

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        hdrs = self._auth_headers()
        hdrs["X-CSRFToken"] = self.csrf_token
        return self._raw_request("POST", path, payload=payload, headers=hdrs)

    def put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        hdrs = self._auth_headers()
        hdrs["X-CSRFToken"] = self.csrf_token
        return self._raw_request("PUT", path, payload=payload, headers=hdrs)


def simple_metric(column_name: str, aggregate: str, label: str) -> dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name},
        "aggregate": aggregate,
        "label": label,
        "hasCustomLabel": True,
    }


def sql_metric(sql_expr: str, label: str) -> dict[str, Any]:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expr,
        "label": label,
        "hasCustomLabel": True,
    }


def temporal_no_filter(col: str = "timestamp_utc") -> list[dict[str, Any]]:
    return [
        {
            "clause": "WHERE",
            "subject": col,
            "operator": "TEMPORAL_RANGE",
            "comparator": "No filter",
            "expressionType": "SIMPLE",
        }
    ]


def build_chart_definitions(ds_ids: dict[str, int]) -> list[dict[str, Any]]:
    fct_id = ds_ids["marts.fct_sensor_readings"]
    sum_id = ds_ids["marts.mart_container_health_summary"]

    return [
        {
            "slice_name": "Total Sensor Readings",
            "datasource_id": fct_id,
            "viz_type": "big_number_total",
            "description": "Total validated telemetry records stored in marts.fct_sensor_readings.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "big_number_total",
                "metric": "count",
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "subheader": "Validated records in marts.fct_sensor_readings",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_format": "smart_date",
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "columns": [],
                        "metrics": ["count"],
                        "order_desc": True,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Active Containers",
            "datasource_id": fct_id,
            "viz_type": "big_number_total",
            "description": "Count of distinct monitored refrigerated containers.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "big_number_total",
                "metric": sql_metric("COUNT(DISTINCT container_id)", "Active Containers"),
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "subheader": "Distinct containers reporting telemetry",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "columns": [],
                        "metrics": [sql_metric("COUNT(DISTINCT container_id)", "Active Containers")],
                        "order_desc": True,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Containers with Statistical Anomalies",
            "datasource_id": sum_id,
            "viz_type": "big_number_total",
            "description": "Containers with |z-score| >= 2.0 vs. initial 12-reading baseline. Statistical anomaly heuristic only — NOT confirmed spoilage.",
            "params": {
                "datasource": f"{sum_id}__table",
                "viz_type": "big_number_total",
                "metric": sql_metric(
                    "COUNT(DISTINCT CASE WHEN statistical_drift_anomaly_count > 0 THEN container_id END)",
                    "Containers with Statistical Anomalies",
                ),
                "adhoc_filters": [],
                "subheader": "Statistical drift (|z| >= 2.0), NOT confirmed spoilage",
                "header_font_size": 0.4,
                "subheader_font_size": 0.14,
                "y_axis_format": "SMART_NUMBER",
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": sum_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [],
                        "columns": [],
                        "metrics": [
                            sql_metric(
                                "COUNT(DISTINCT CASE WHEN statistical_drift_anomaly_count > 0 THEN container_id END)",
                                "Containers with Statistical Anomalies",
                            )
                        ],
                        "order_desc": True,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Average Temperature (°C)",
            "datasource_id": fct_id,
            "viz_type": "big_number_total",
            "description": "Mean container temperature (°C) across filtered sensor readings.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "big_number_total",
                "metric": simple_metric("temperature_c", "AVG", "Avg Temperature (°C)"),
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "subheader": "°C average across readings",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",.2f",
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "columns": [],
                        "metrics": [simple_metric("temperature_c", "AVG", "Avg Temperature (°C)")],
                        "order_desc": True,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Temperature Trend (°C)",
            "datasource_id": fct_id,
            "viz_type": "echarts_timeseries_line",
            "description": "Hourly average temperature (°C) per container over UTC time.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "echarts_timeseries_line",
                "x_axis": "timestamp_utc",
                "time_grain_sqla": "PT1H",
                "metrics": [simple_metric("temperature_c", "AVG", "Avg Temperature (°C)")],
                "groupby": ["container_id"],
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "row_limit": 5000,
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "legendMargin": 15,
                "x_axis_title": "Timestamp (UTC)",
                "x_axis_title_margin": 75,
                "y_axis_title": "Temperature (°C)",
                "y_axis_title_margin": 50,
                "y_axis_title_position": "Left",
                "xAxisLabelRotation": 30,
                "x_axis_time_format": "%b %d %H:%M",
                "truncateXAxis": False,
                "y_axis_format": ",.2f",
                "markerEnabled": True,
                "markerSize": 5,
                "rich_tooltip": True,
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "extras": {"time_grain_sqla": "PT1H", "having": "", "where": ""},
                        "columns": [
                            {"timeGrain": "PT1H", "columnType": "BASE_AXIS", "sqlExpression": "timestamp_utc", "label": "timestamp_utc", "expressionType": "SQL"},
                            "container_id",
                        ],
                        "metrics": [simple_metric("temperature_c", "AVG", "Avg Temperature (°C)")],
                        "orderby": [[simple_metric("temperature_c", "AVG", "Avg Temperature (°C)"), False]],
                        "row_limit": 5000,
                        "series_columns": ["container_id"],
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Humidity Trend (%)",
            "datasource_id": fct_id,
            "viz_type": "echarts_timeseries_line",
            "description": "Hourly average relative humidity (%) per container over UTC time.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "echarts_timeseries_line",
                "x_axis": "timestamp_utc",
                "time_grain_sqla": "PT1H",
                "metrics": [simple_metric("humidity_pct", "AVG", "Avg Humidity (%)")],
                "groupby": ["container_id"],
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "row_limit": 5000,
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "legendMargin": 15,
                "x_axis_title": "Timestamp (UTC)",
                "x_axis_title_margin": 75,
                "y_axis_title": "Humidity (%)",
                "y_axis_title_margin": 50,
                "y_axis_title_position": "Left",
                "xAxisLabelRotation": 30,
                "x_axis_time_format": "%b %d %H:%M",
                "truncateXAxis": False,
                "y_axis_format": ",.2f",
                "markerEnabled": True,
                "markerSize": 5,
                "rich_tooltip": True,
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "extras": {"time_grain_sqla": "PT1H", "having": "", "where": ""},
                        "columns": [
                            {"timeGrain": "PT1H", "columnType": "BASE_AXIS", "sqlExpression": "timestamp_utc", "label": "timestamp_utc", "expressionType": "SQL"},
                            "container_id",
                        ],
                        "metrics": [simple_metric("humidity_pct", "AVG", "Avg Humidity (%)")],
                        "orderby": [[simple_metric("humidity_pct", "AVG", "Avg Humidity (%)"), False]],
                        "row_limit": 5000,
                        "series_columns": ["container_id"],
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Container-wise Temperature Comparison",
            "datasource_id": fct_id,
            "viz_type": "echarts_timeseries_bar",
            "description": "Comparison of average temperature (°C) across refrigerated containers.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "container_id",
                "metrics": [simple_metric("temperature_c", "AVG", "Avg Temperature (°C)")],
                "groupby": [],
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "row_limit": 100,
                "show_legend": False,
                "legendType": "scroll",
                "legendOrientation": "top",
                "legendMargin": 15,
                "show_value": True,
                "x_axis_title": "Container ID",
                "x_axis_title_margin": 35,
                "y_axis_title": "Avg Temperature (°C)",
                "y_axis_title_margin": 45,
                "y_axis_title_position": "Left",
                "x_axis_label_rotation": 0,
                "truncateXAxis": False,
                "y_axis_format": ",.2f",
                "rich_tooltip": True,
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "columns": ["container_id"],
                        "metrics": [simple_metric("temperature_c", "AVG", "Avg Temperature (°C)")],
                        "orderby": [[simple_metric("temperature_c", "AVG", "Avg Temperature (°C)"), False]],
                        "row_limit": 100,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Vibration Analysis by Container",
            "datasource_id": fct_id,
            "viz_type": "echarts_timeseries_bar",
            "description": "Average and peak vibration (g) across containers.",
            "params": {
                "datasource": f"{fct_id}__table",
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "container_id",
                "metrics": [
                    simple_metric("vibration_g", "AVG", "Avg Vibration (g)"),
                    simple_metric("vibration_g", "MAX", "Max Vibration (g)"),
                ],
                "groupby": [],
                "adhoc_filters": temporal_no_filter("timestamp_utc"),
                "row_limit": 100,
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "legendMargin": 15,
                "x_axis_title": "Container ID",
                "x_axis_title_margin": 35,
                "y_axis_title": "Vibration (g)",
                "y_axis_title_margin": 45,
                "y_axis_title_position": "Left",
                "x_axis_label_rotation": 0,
                "truncateXAxis": False,
                "y_axis_format": ",.2f",
                "rich_tooltip": True,
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": fct_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [{"col": "timestamp_utc", "op": "TEMPORAL_RANGE", "val": "No filter"}],
                        "columns": ["container_id"],
                        "metrics": [
                            simple_metric("vibration_g", "AVG", "Avg Vibration (g)"),
                            simple_metric("vibration_g", "MAX", "Max Vibration (g)"),
                        ],
                        "orderby": [[simple_metric("vibration_g", "AVG", "Avg Vibration (g)"), False]],
                        "row_limit": 100,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Statistical Anomaly Comparison",
            "datasource_id": sum_id,
            "viz_type": "echarts_timeseries_bar",
            "description": "Count of readings exceeding |z-score| >= 2.0 vs. initial 12-reading baseline. Statistical anomaly heuristic only — NOT confirmed spoilage.",
            "params": {
                "datasource": f"{sum_id}__table",
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "container_id",
                "metrics": [
                    simple_metric("statistical_temp_drift_count", "SUM", "Temp Drift Count"),
                    simple_metric("statistical_humidity_drift_count", "SUM", "Humidity Drift Count"),
                    simple_metric("statistical_drift_anomaly_count", "SUM", "Total Anomaly Readings"),
                ],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 100,
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "legendMargin": 15,
                "x_axis_title": "Container ID",
                "x_axis_title_margin": 35,
                "y_axis_title": "Anomaly Reading Count",
                "y_axis_title_margin": 45,
                "y_axis_title_position": "Left",
                "x_axis_label_rotation": 0,
                "truncateXAxis": False,
                "y_axis_format": "SMART_NUMBER",
                "rich_tooltip": True,
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": sum_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [],
                        "columns": ["container_id"],
                        "metrics": [
                            simple_metric("statistical_temp_drift_count", "SUM", "Temp Drift Count"),
                            simple_metric("statistical_humidity_drift_count", "SUM", "Humidity Drift Count"),
                            simple_metric("statistical_drift_anomaly_count", "SUM", "Total Anomaly Readings"),
                        ],
                        "orderby": [[simple_metric("statistical_drift_anomaly_count", "SUM", "Total Anomaly Readings"), False]],
                        "row_limit": 100,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
        {
            "slice_name": "Container Health Summary",
            "datasource_id": sum_id,
            "viz_type": "table",
            "description": "Per-container latest readings, baseline drift, z-scores, and statistical anomaly counts (NOT confirmed spoilage).",
            "params": {
                "datasource": f"{sum_id}__table",
                "viz_type": "table",
                "query_mode": "raw",
                "all_columns": [
                    "container_id",
                    "product",
                    "route",
                    "total_readings",
                    "latest_reading_utc",
                    "latest_temp_c",
                    "latest_humidity_pct",
                    "latest_vibration_g",
                    "baseline_avg_temp_c",
                    "latest_temp_drift_c",
                    "latest_humidity_drift_pct",
                    "latest_temp_zscore",
                    "statistical_drift_anomaly_count",
                ],
                "order_by_cols": ['["statistical_drift_anomaly_count", false]'],
                "adhoc_filters": [],
                "row_limit": 100,
                "page_length": 10,
                "include_search": True,
                "show_cell_bars": True,
                "align_pn": False,
                "table_timestamp_format": "%Y-%m-%d %H:%M",
                "extra_form_data": {},
            },
            "query_context": {
                "datasource": {"id": sum_id, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "filters": [],
                        "columns": [
                            "container_id",
                            "product",
                            "route",
                            "total_readings",
                            "latest_reading_utc",
                            "latest_temp_c",
                            "latest_humidity_pct",
                            "latest_vibration_g",
                            "baseline_avg_temp_c",
                            "latest_temp_drift_c",
                            "latest_humidity_drift_pct",
                            "latest_temp_zscore",
                            "statistical_drift_anomaly_count",
                        ],
                        "orderby": [["statistical_drift_anomaly_count", False]],
                        "row_limit": 100,
                    }
                ],
                "result_format": "json",
                "result_type": "full",
            },
        },
    ]


def ensure_chart(client: SupersetClient, spec: dict[str, Any], existing_by_name: dict[str, dict[str, Any]]) -> int:
    name = spec["slice_name"]
    if name in existing_by_name:
        chart_id = int(existing_by_name[name]["id"])
        print(f"REUSE_CHART id={chart_id}: {name}")
        return chart_id

    payload = {
        "slice_name": name,
        "datasource_id": spec["datasource_id"],
        "datasource_type": "table",
        "viz_type": spec["viz_type"],
        "description": spec.get("description", ""),
        "params": json.dumps(spec["params"]),
        "query_context": json.dumps(spec["query_context"]),
    }
    created = client.post("/api/v1/chart/", payload)
    chart_id = int(created["id"])
    print(f"CREATED_CHART id={chart_id}: {name} ({spec['viz_type']})")
    return chart_id


def verify_chart_query(client: SupersetClient, chart_id: int, spec: dict[str, Any]) -> list[dict[str, Any]]:
    qc = dict(spec["query_context"])
    qc["force"] = True
    resp = client.post("/api/v1/chart/data", qc)
    results = resp.get("result", [])
    if not results:
        raise RuntimeError(f"Chart id={chart_id} ({spec['slice_name']}) returned empty result list")
    first = results[0]
    if first.get("error"):
        raise RuntimeError(f"Chart id={chart_id} ({spec['slice_name']}) SQL/query error: {first['error']}")
    data_rows = first.get("data", [])
    print(f"CHART_DATA_OK id={chart_id} ({spec['slice_name']}): rows={len(data_rows)} sample={data_rows[:2]}")
    return data_rows


def build_dashboard_layout(chart_ids: dict[str, int]) -> tuple[dict[str, Any], dict[str, Any]]:
    def chart_node(node_id: str, parent_id: str, cid: int, name: str, width: int, height: int) -> dict[str, Any]:
        return {
            "id": node_id,
            "type": "CHART",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", parent_id],
            "meta": {
                "chartId": cid,
                "sliceName": name,
                "width": width,
                "height": height,
                "uuid": str(uuid4()),
            },
        }

    md_id = "MARKDOWN-NOTE"
    row_note = "ROW-NOTE"
    row_kpi = "ROW-KPI"
    row_trends = "ROW-TRENDS"
    row_compare = "ROW-COMPARE"
    row_anomalies = "ROW-ANOMALIES"
    row_table = "ROW-TABLE"

    c_total = ("CHART-KPI-TOTAL", row_kpi, chart_ids["Total Sensor Readings"], "Total Sensor Readings", 3, 20)
    c_active = ("CHART-KPI-ACTIVE", row_kpi, chart_ids["Active Containers"], "Active Containers", 3, 20)
    c_anom = ("CHART-KPI-ANOM", row_kpi, chart_ids["Containers with Statistical Anomalies"], "Containers with Statistical Anomalies", 3, 20)
    c_temp = ("CHART-KPI-TEMP", row_kpi, chart_ids["Average Temperature (°C)"], "Average Temperature (°C)", 3, 20)

    c_t_trend = ("CHART-TEMP-TREND", row_trends, chart_ids["Temperature Trend (°C)"], "Temperature Trend (°C)", 6, 60)
    c_h_trend = ("CHART-HUM-TREND", row_trends, chart_ids["Humidity Trend (%)"], "Humidity Trend (%)", 6, 60)

    c_t_bar = ("CHART-TEMP-BAR", row_compare, chart_ids["Container-wise Temperature Comparison"], "Container-wise Temperature Comparison", 6, 50)
    c_v_bar = ("CHART-VIB-BAR", row_compare, chart_ids["Vibration Analysis by Container"], "Vibration Analysis by Container", 6, 50)
    c_a_bar = ("CHART-ANOM-BAR", row_anomalies, chart_ids["Statistical Anomaly Comparison"], "Statistical Anomaly Comparison", 12, 48)

    c_table = ("CHART-HEALTH-TBL", row_table, chart_ids["Container Health Summary"], "Container Health Summary", 12, 54)

    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": [row_note, row_kpi, row_trends, row_compare, row_anomalies, row_table],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "id": "HEADER_ID",
            "type": "HEADER",
            "meta": {"text": DASHBOARD_TITLE},
        },
        row_note: {
            "id": row_note,
            "type": "ROW",
            "children": [md_id],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        md_id: {
            "id": md_id,
            "type": "MARKDOWN",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_note],
            "meta": {
                "width": 12,
                "height": 16,
                "code": (
                    "### AtmoSync Cold-Chain Telemetry Overview\n"
                    "Source: PostgreSQL `marts` (`dim_containers`, `fct_sensor_readings`, `mart_container_health_summary`).\n\n"
                    "**Methodology Note:** Statistical drift metrics (`|z-score| >= 2.0` relative to each container's initial 12-reading baseline) "
                    "are **statistical anomaly heuristics** and do **not** represent confirmed biological spoilage."
                ),
            },
        },
        row_kpi: {
            "id": row_kpi,
            "type": "ROW",
            "children": [c_total[0], c_active[0], c_anom[0], c_temp[0]],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        row_trends: {
            "id": row_trends,
            "type": "ROW",
            "children": [c_t_trend[0], c_h_trend[0]],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        row_compare: {
            "id": row_compare,
            "type": "ROW",
            "children": [c_t_bar[0], c_v_bar[0]],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        row_anomalies: {
            "id": row_anomalies,
            "type": "ROW",
            "children": [c_a_bar[0]],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        row_table: {
            "id": row_table,
            "type": "ROW",
            "children": [c_table[0]],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }

    for node_id, parent_id, cid, name, width, height in [
        c_total,
        c_active,
        c_anom,
        c_temp,
        c_t_trend,
        c_h_trend,
        c_t_bar,
        c_v_bar,
        c_a_bar,
        c_table,
    ]:
        position[node_id] = chart_node(node_id, parent_id, cid, name, width, height)

    fct_chart_ids = [
        chart_ids["Total Sensor Readings"],
        chart_ids["Active Containers"],
        chart_ids["Average Temperature (°C)"],
        chart_ids["Temperature Trend (°C)"],
        chart_ids["Humidity Trend (%)"],
        chart_ids["Container-wise Temperature Comparison"],
        chart_ids["Vibration Analysis by Container"],
    ]
    summary_chart_ids = [
        chart_ids["Containers with Statistical Anomalies"],
        chart_ids["Statistical Anomaly Comparison"],
        chart_ids["Container Health Summary"],
    ]

    metadata = {
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "color_scheme": "supersetColors",
        "label_colors": {},
        "shared_label_colors": {},
        "cross_filters_enabled": True,
        "native_filter_configuration": [
            {
                "id": "NATIVE_FILTER-CONTAINER",
                "name": "Container ID",
                "filterType": "filter_select",
                "targets": [{"datasetId": 1, "column": {"name": "container_id"}}],
                "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                "controlValues": {"enableEmptyFilter": False, "multiSelect": True, "searchAllOptions": False, "inverseSelection": False},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "description": "Filter all charts by container_id.",
            },
            {
                "id": "NATIVE_FILTER-PRODUCT",
                "name": "Product",
                "filterType": "filter_select",
                "targets": [{"datasetId": 3, "column": {"name": "product"}}],
                "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                "controlValues": {"enableEmptyFilter": False, "multiSelect": True, "searchAllOptions": False, "inverseSelection": False},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": fct_chart_ids},
                "type": "NATIVE_FILTER",
                "description": "Filter container health summary charts by product (marts.mart_container_health_summary).",
            },
            {
                "id": "NATIVE_FILTER-ROUTE",
                "name": "Route",
                "filterType": "filter_select",
                "targets": [{"datasetId": 3, "column": {"name": "route"}}],
                "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                "controlValues": {"enableEmptyFilter": False, "multiSelect": True, "searchAllOptions": False, "inverseSelection": False},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": fct_chart_ids},
                "type": "NATIVE_FILTER",
                "description": "Filter container health summary charts by route (marts.mart_container_health_summary).",
            },
            {
                "id": "NATIVE_FILTER-TIMERANGE",
                "name": "Time Range",
                "filterType": "filter_time",
                "targets": [{"datasetId": 2}],
                "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                "controlValues": {},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": summary_chart_ids},
                "type": "NATIVE_FILTER",
                "description": "Filter fact-table charts by UTC timestamp range.",
            },
        ],
        "chart_configuration": {},
    }
    return position, metadata


def main() -> int:
    env = dotenv_values(PROJECT_ROOT / ".env")
    client = SupersetClient(
        BASE_URL,
        env["SUPERSET_ADMIN_USERNAME"] or "admin",
        env["SUPERSET_ADMIN_PASSWORD"] or "",
    )

    datasets_resp = client.get("/api/v1/dataset/?q=(page_size:50)")
    ds_ids = {
        f"{d['schema']}.{d['table_name']}": int(d["id"])
        for d in datasets_resp.get("result", [])
    }
    for required in ("marts.dim_containers", "marts.fct_sensor_readings", "marts.mart_container_health_summary"):
        if required not in ds_ids:
            raise RuntimeError(f"Missing required registered dataset: {required}")

    existing_charts_resp = client.get("/api/v1/chart/?q=(page_size:100)")
    existing_by_name = {c["slice_name"]: c for c in existing_charts_resp.get("result", [])}

    specs = build_chart_definitions(ds_ids)
    chart_ids: dict[str, int] = {}
    for spec in specs:
        cid = ensure_chart(client, spec, existing_by_name)
        verify_chart_query(client, cid, spec)
        chart_ids[spec["slice_name"]] = cid

    existing_dash_resp = client.get("/api/v1/dashboard/?q=(page_size:50)")
    existing_dash = next(
        (d for d in existing_dash_resp.get("result", []) if d.get("dashboard_title") == DASHBOARD_TITLE),
        None,
    )
    position, metadata = build_dashboard_layout(chart_ids)

    if existing_dash is not None:
        dash_id = int(existing_dash["id"])
        print(f"REUSE_DASHBOARD id={dash_id}: {DASHBOARD_TITLE}")
    else:
        created_dash = client.post(
            "/api/v1/dashboard/",
            {
                "dashboard_title": DASHBOARD_TITLE,
                "slug": DASHBOARD_SLUG,
                "published": True,
                "position_json": json.dumps(position),
                "json_metadata": json.dumps(metadata),
            },
        )
        dash_id = int(created_dash["id"])
        print(f"CREATED_DASHBOARD id={dash_id}: {DASHBOARD_TITLE}")

    # Ensure each chart links to this dashboard cleanly (append-only, preserving any other dashboards).
    for spec in specs:
        cid = chart_ids[spec["slice_name"]]
        detail = client.get(f"/api/v1/chart/{cid}")["result"]
        linked_ids = [int(d["id"]) for d in detail.get("dashboards", [])]
        if dash_id not in linked_ids:
            linked_ids.append(dash_id)
            client.put(f"/api/v1/chart/{cid}", {"dashboards": linked_ids})

    print(f"DASHBOARD_URL: {BASE_URL}/superset/dashboard/{dash_id}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
