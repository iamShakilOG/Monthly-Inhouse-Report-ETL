#!/usr/bin/env python3
"""Build in-house member activity and hour statistics from Google Sheets.

This script converts the notebook workflow into a production-ready Python program
with structured logging, validation, reusable functions, and configurable inputs.

Main steps:
1. Read the attendance sheet and calculate monthly active days and office hours.
2. Read the merged report sheet and aggregate monthly metrics per QAI ID.
3. Merge attendance and report outputs.
4. Build a detailed breakdown sheet and a summary sheet.
5. Upload both outputs to the destination Google Spreadsheet.

Environment variables supported:
- GOOGLE_CREDS_FILE
- DELIVERY_SHEET_KEY
- DELIVERY_WORKSHEET_NAME
- REPORT_SHEET_KEY
- REPORT_WORKSHEET_NAME
- OUTPUT_SHEET_KEY
- OUTPUT_BREAKDOWN_WORKSHEET
- OUTPUT_SUMMARY_WORKSHEET
- REPORT_YEAR_FILTER
- LOG_LEVEL
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Sequence, Tuple

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOGGER = logging.getLogger("inhouse_member_stats")

GOOGLE_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

NUMERIC_COLUMNS = [
    "Effective Work Hour",
    "Final Working Hour",
    "Client Billing Hours",
    "Annotation Time (Minutes)",
    "QA Time (Minutes)",
    "Crosscheck Time (Minutes)",
    "Meeting Time (Minutes)",
    "Project Study (Minutes)",
    "Resource Training (Minutes) - This section is for lead",
    "Q&A Group support (Minutes)",
    "Documentation (Minutes)",
    "Demo (Minutes)",
    "Break Time (Minutes)",
    "Server Downtime (Minutes)",
    "Free time (Minutes)",
    "Total Logged Hours",
]

ID_COLUMNS = ["QAI ID", "Full Name", "Resource Type", "Resource Allocation"]
BREAKDOWN_ID_COLUMNS = ["QAI ID", "Full Name", "Joining Date", "Resource Type", "Resource Allocation"]
REPORT_REQUIRED_COLUMNS = ["REPORT_MONTH", *ID_COLUMNS, *NUMERIC_COLUMNS]

PRIORITY_ORDER = [
    "Effective Work Hour",
    "Final Working Hour",
    "Client Billing Hours",
    "Total Logged Hours",
    "Annotation Time (Minutes)",
    "QA Time (Minutes)",
    "Crosscheck Time (Minutes)",
    "Meeting Time (Minutes)",
    "Project Study (Minutes)",
    "Resource Training (Minutes)",
    "Q&A Group support (Minutes)",
    "Documentation (Minutes)",
    "Demo (Minutes)",
    "Break Time (Minutes)",
    "Server Downtime (Minutes)",
    "Free time (Minutes)",
    "Active Days",
    "Active Days Office Hours",
]

PRODUCTION_SOURCE_METRICS = [
    "Annotation Time (Minutes)",
    "QA Time (Minutes)",
]

OTHER_TIME_SOURCE_METRICS = [
    "Crosscheck Time (Minutes)",
    "Meeting Time (Minutes)",
    "Project Study (Minutes)",
    "Resource Training (Minutes) - This section is for lead",
    "Q&A Group support (Minutes)",
    "Documentation (Minutes)",
    "Demo (Minutes)",
    "Break Time (Minutes)",
    "Server Downtime (Minutes)",
    "Free time (Minutes)",
]


@dataclass(frozen=True)
class Config:
    creds_file: str
    delivery_sheet_key: str
    delivery_worksheet_name: str
    report_sheet_key: str
    report_worksheet_name: str
    output_sheet_key: str
    output_breakdown_worksheet: str
    output_summary_worksheet: str
    report_year_filter: int
    log_level: str = "INFO"


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""


class DataValidationError(Exception):
    """Raised when source sheets do not contain the expected structure."""


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=LOG_FORMAT,
        stream=sys.stdout,
    )


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Build in-house member active days and effective hour stats."
    )
    parser.add_argument(
        "--creds-file",
        default=os.getenv("GOOGLE_CREDS_FILE", "hip-lightning-451508-e5-c80ef62ddcea.json"),
        help="Path to Google service account JSON credentials file.",
    )
    parser.add_argument(
        "--delivery-sheet-key",
        default=os.getenv("DELIVERY_SHEET_KEY", "1YgIGvaN0NA6M2k5oHSJF-m0S8A5EhC6OkTCYfjT3bjw"),
        help="Google Sheet key for the attendance source.",
    )
    parser.add_argument(
        "--delivery-worksheet-name",
        default=os.getenv("DELIVERY_WORKSHEET_NAME", "Team List & Activity"),
        help="Worksheet name for the attendance source.",
    )
    parser.add_argument(
        "--report-sheet-key",
        default=os.getenv("REPORT_SHEET_KEY", "1lqZJeOg9pTzvfHJfhMjPS7qNsZukUSBUG5pVyn_SiPg"),
        help="Google Sheet key for the merged report source.",
    )
    parser.add_argument(
        "--report-worksheet-name",
        default=os.getenv("REPORT_WORKSHEET_NAME", "Merged"),
        help="Worksheet name for the merged report source.",
    )
    parser.add_argument(
        "--output-sheet-key",
        default=os.getenv("OUTPUT_SHEET_KEY", "1IikdQL_2hwlOrqm0JZOdQ_DxZkCa012X_CsJmycmsi0"),
        help="Google Sheet key for the upload target.",
    )
    parser.add_argument(
        "--output-breakdown-worksheet",
        default=os.getenv("OUTPUT_BREAKDOWN_WORKSHEET", "Total Breakdown"),
        help="Worksheet name for the detailed output.",
    )
    parser.add_argument(
        "--output-summary-worksheet",
        default=os.getenv("OUTPUT_SUMMARY_WORKSHEET", "Summary"),
        help="Worksheet name for the summary output.",
    )
    parser.add_argument(
        "--report-year-filter",
        type=int,
        default=int(os.getenv("REPORT_YEAR_FILTER", "2026")),
        help="Only keep report months for this year.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level, e.g. DEBUG, INFO, WARNING.",
    )

    args = parser.parse_args()

    return Config(
        creds_file=args.creds_file,
        delivery_sheet_key=args.delivery_sheet_key,
        delivery_worksheet_name=args.delivery_worksheet_name,
        report_sheet_key=args.report_sheet_key,
        report_worksheet_name=args.report_worksheet_name,
        output_sheet_key=args.output_sheet_key,
        output_breakdown_worksheet=args.output_breakdown_worksheet,
        output_summary_worksheet=args.output_summary_worksheet,
        report_year_filter=args.report_year_filter,
        log_level=args.log_level,
    )


def validate_config(config: Config) -> None:
    if not os.path.isfile(config.creds_file):
        raise ConfigurationError(f"Credentials file not found: {config.creds_file}")

    required_values = {
        "delivery_sheet_key": config.delivery_sheet_key,
        "delivery_worksheet_name": config.delivery_worksheet_name,
        "report_sheet_key": config.report_sheet_key,
        "report_worksheet_name": config.report_worksheet_name,
        "output_sheet_key": config.output_sheet_key,
        "output_breakdown_worksheet": config.output_breakdown_worksheet,
        "output_summary_worksheet": config.output_summary_worksheet,
    }
    missing = [key for key, value in required_values.items() if not str(value).strip()]
    if missing:
        raise ConfigurationError(f"Missing configuration values: {', '.join(missing)}")


def authorize_gspread(creds_file: str) -> gspread.Client:
    LOGGER.info("Authorizing Google Sheets client")
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        creds_file,
        GOOGLE_SCOPES,
    )
    return gspread.authorize(credentials)


def fetch_worksheet_as_dataframe(
    client: gspread.Client,
    sheet_key: str,
    worksheet_name: str,
) -> pd.DataFrame:
    LOGGER.info("Reading worksheet '%s' from sheet %s", worksheet_name, sheet_key)
    worksheet = client.open_by_key(sheet_key).worksheet(worksheet_name)
    values = worksheet.get_all_values()

    if not values:
        raise DataValidationError(
            f"Worksheet '{worksheet_name}' in sheet '{sheet_key}' is empty."
        )

    if len(values) == 1:
        LOGGER.warning(
            "Worksheet '%s' in sheet %s contains headers only and no data rows.",
            worksheet_name,
            sheet_key,
        )

    header = values[0]
    rows = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=header)
    LOGGER.info("Loaded %s rows and %s columns", len(df), len(df.columns))
    return df


def require_columns(df: pd.DataFrame, required_columns: Sequence[str], label: str) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise DataValidationError(
            f"Missing required columns in {label}: {', '.join(missing)}"
        )


def normalize_joining_date(value: object) -> str:
    if value is None:
        return ""
    value_str = str(value).strip()
    if not value_str:
        return ""

    parsed = pd.to_datetime(value_str, errors="coerce")
    if pd.isna(parsed):
        return value_str

    return parsed.strftime("%Y-%m-%d")


def build_inhouse_activity_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    require_columns(df, ["QAI ID", "Joining Date"], "attendance worksheet")
    LOGGER.info("Calculating active days and office hours from attendance sheet")

    date_columns: Dict[str, datetime] = {}
    for column in df.columns:
        try:
            date_columns[column] = datetime.strptime(column.strip(), "%m/%d/%Y")
        except ValueError:
            continue

    if not date_columns:
        raise DataValidationError(
            "No attendance date columns were found with format MM/DD/YYYY."
        )

    month_groups: Dict[Tuple[int, int], List[str]] = defaultdict(list)
    for column, parsed_date in date_columns.items():
        month_groups[(parsed_date.year, parsed_date.month)].append(column)

    result_rows = []
    for _, row in df.iterrows():
        qai_id = str(row.get("QAI ID", "")).strip()
        if not qai_id:
            continue

        record = {
            "QAI ID": qai_id,
            "Joining Date": normalize_joining_date(row.get("Joining Date", "")),
        }

        for year_month, columns in sorted(month_groups.items()):
            year, month = year_month
            month_label = f"{datetime(year, month, 1).strftime('%B - %Y')} Active Days"
            present_days = sum(
                1
                for column in columns
                if str(row.get(column, "")).strip()
                and str(row.get(column, "")).strip().lower() != "unavailable"
            )
            record[month_label] = present_days
            record[f"{month_label} Office Hours"] = present_days * 8

        result_rows.append(record)

    result_df = pd.DataFrame(result_rows)
    LOGGER.info("Built attendance output with %s rows", len(result_df))
    return result_df


def parse_report_month(value: str) -> str | None:
    try:
        dt = datetime.strptime(str(value).strip(), "%Y-%m")
        return dt.strftime("%B - %Y")
    except ValueError:
        return None


def build_report_metrics_dataframe(df: pd.DataFrame, report_year_filter: int) -> pd.DataFrame:
    require_columns(df, REPORT_REQUIRED_COLUMNS, "merged report worksheet")
    LOGGER.info("Cleaning and aggregating merged report data for year %s", report_year_filter)

    report_df = df[REPORT_REQUIRED_COLUMNS].copy()
    report_df["QAI ID"] = report_df["QAI ID"].astype(str).str.strip()
    report_df["Full Name"] = report_df["Full Name"].astype(str).str.strip()
    report_df["Resource Type"] = report_df["Resource Type"].astype(str).str.strip()
    report_df["Resource Allocation"] = report_df["Resource Allocation"].astype(str).str.strip()
    report_df["REPORT_MONTH"] = report_df["REPORT_MONTH"].astype(str).str.strip()

    for column in NUMERIC_COLUMNS:
        report_df[column] = pd.to_numeric(report_df[column], errors="coerce").fillna(0)

    before_drop = len(report_df)
    report_df = report_df[
        report_df["QAI ID"].ne("") & report_df["REPORT_MONTH"].ne("")
    ].copy()
    LOGGER.info("Removed %s rows with empty QAI ID or REPORT_MONTH", before_drop - len(report_df))

    report_df["month_label"] = report_df["REPORT_MONTH"].apply(parse_report_month)
    report_df = report_df[report_df["month_label"].notna()].copy()
    report_df = report_df[
        report_df["month_label"].str.endswith(str(report_year_filter))
    ].copy()

    if report_df.empty:
        raise DataValidationError(
            f"No valid merged report rows found for year {report_year_filter}."
        )

    aggregated = (
        report_df.groupby([*ID_COLUMNS, "month_label"], as_index=False)[NUMERIC_COLUMNS]
        .sum()
    )
    LOGGER.info("Aggregated report data into %s grouped rows", len(aggregated))

    pivot_frames = []
    for metric in NUMERIC_COLUMNS:
        pivot = aggregated.pivot_table(
            index=ID_COLUMNS,
            columns="month_label",
            values=metric,
            aggfunc="sum",
        ).rename(columns=lambda month: f"{month}_{metric}")
        pivot_frames.append(pivot)

    result_df = pd.concat(pivot_frames, axis=1)
    all_months = sorted(
        {column.split("_")[0] for column in result_df.columns},
        key=lambda month: datetime.strptime(month, "%B - %Y"),
    )
    ordered_cols = [f"{month}_{metric}" for month in all_months for metric in NUMERIC_COLUMNS]
    result_df = result_df.reindex(columns=ordered_cols).fillna(0).reset_index()

    LOGGER.info(
        "Built report metrics output with %s rows and %s columns",
        len(result_df),
        len(result_df.columns),
    )
    return result_df


def month_sort_key(month_string: str) -> pd.Timestamp:
    return pd.to_datetime(month_string, format="%B - %Y", errors="coerce")


def build_final_breakdown_dataframe(
    report_metrics_df: pd.DataFrame,
    activity_df: pd.DataFrame,
) -> pd.DataFrame:
    require_columns(report_metrics_df, ["QAI ID"], "report metrics dataframe")
    require_columns(activity_df, ["QAI ID", "Joining Date"], "activity dataframe")
    LOGGER.info("Merging report metrics and attendance activity")

    merged_df = pd.merge(report_metrics_df, activity_df, on="QAI ID", how="left")

    columns = [column for column in merged_df.columns if column not in {"QAI ID", "Full Name", "Joining Date", "Resource Type", "Resource Allocation"}]
    month_groups: Dict[str, List[str]] = defaultdict(list)
    for column in columns:
        month_key = column.split("_")[0].split(" Active Days")[0].strip()
        month_groups[month_key].append(column)

    sorted_months = sorted(month_groups.keys(), key=month_sort_key)
    ordered_columns = BREAKDOWN_ID_COLUMNS.copy()

    def column_priority(column_name: str) -> int:
        for index, metric_name in enumerate(PRIORITY_ORDER):
            if metric_name in column_name:
                return index
        return len(PRIORITY_ORDER)

    for month in sorted_months:
        ordered_columns.extend(sorted(month_groups[month], key=column_priority))

    final_df = merged_df.reindex(columns=ordered_columns)
    LOGGER.info(
        "Built final breakdown dataframe with %s rows and %s columns",
        len(final_df),
        len(final_df.columns),
    )
    return final_df


def build_summary_dataframe(final_df: pd.DataFrame) -> pd.DataFrame:
    require_columns(final_df, BREAKDOWN_ID_COLUMNS, "final breakdown dataframe")
    LOGGER.info("Building row-wise monthly summary dataframe")

    all_columns = list(final_df.columns)
    month_names = sorted(
        {
            column.split("_")[0].split(" Active Days")[0].strip()
            for column in all_columns
            if column not in BREAKDOWN_ID_COLUMNS
        },
        key=month_sort_key,
    )

    rows: List[Dict[str, object]] = []

    for _, row in final_df.iterrows():
        base_record = {
            "QAI ID": str(row.get("QAI ID", "")).strip(),
            "Full Name": str(row.get("Full Name", "")).strip(),
            "Joining Date": str(row.get("Joining Date", "")).strip(),
            "Resource Type": str(row.get("Resource Type", "")).strip(),
            "Resource Allocation": str(row.get("Resource Allocation", "")).strip(),
        }

        for month in month_names:
            effective_col = f"{month}_Effective Work Hour"
            active_days_col = f"{month} Active Days"
            active_hour_col = f"{month} Active Days Office Hours"

            effective_hours = pd.to_numeric(row.get(effective_col, 0), errors="coerce")
            if pd.isna(effective_hours):
                effective_hours = 0.0

            production_hours = 0.0
            for metric in PRODUCTION_SOURCE_METRICS:
                col = f"{month}_{metric}"
                value = pd.to_numeric(row.get(col, 0), errors="coerce")
                if pd.isna(value):
                    value = 0.0
                production_hours += float(value)

            other_hours = 0.0
            for metric in OTHER_TIME_SOURCE_METRICS:
                col = f"{month}_{metric}"
                value = pd.to_numeric(row.get(col, 0), errors="coerce")
                if pd.isna(value):
                    value = 0.0
                other_hours += float(value)

            active_days = pd.to_numeric(row.get(active_days_col, 0), errors="coerce")
            if pd.isna(active_days):
                active_days = 0.0

            active_hour = pd.to_numeric(row.get(active_hour_col, 0), errors="coerce")
            if pd.isna(active_hour):
                active_hour = 0.0

            summary_row = {
                **base_record,
                "Month": month,
                "Effective Hours": float(effective_hours),
                "Production Hours": production_hours,
                "Other Time Tracking Hours": other_hours,
                "Active Days": float(active_days),
                "Active Hour": float(active_hour),
            }

            if any(
                summary_row[col] != 0
                for col in [
                    "Effective Hours",
                    "Production Hours",
                    "Other Time Tracking Hours",
                    "Active Days",
                    "Active Hour",
                ]
            ):
                rows.append(summary_row)

    summary_df = pd.DataFrame(rows)

    if summary_df.empty:
        summary_df = pd.DataFrame(
            columns=[
                "QAI ID",
                "Full Name",
                "Joining Date",
                "Resource Type",
                "Resource Allocation",
                "Month",
                "Effective Hours",
                "Production Hours",
                "Other Time Tracking Hours",
                "Active Days",
                "Active Hour",
            ]
        )
    else:
        summary_df["Month_sort"] = pd.to_datetime(
            summary_df["Month"], format="%B - %Y", errors="coerce"
        )
        summary_df = summary_df.sort_values(
            by=["QAI ID", "Month_sort", "Full Name"],
            kind="stable",
        ).drop(columns=["Month_sort"])

        summary_df = summary_df[
            [
                "QAI ID",
                "Full Name",
                "Joining Date",
                "Resource Type",
                "Resource Allocation",
                "Month",
                "Effective Hours",
                "Production Hours",
                "Other Time Tracking Hours",
                "Active Days",
                "Active Hour",
            ]
        ].reset_index(drop=True)

    LOGGER.info(
        "Built summary dataframe with %s rows and %s columns",
        len(summary_df),
        len(summary_df.columns),
    )
    return summary_df


def upload_dataframe(
    client: gspread.Client,
    sheet_key: str,
    worksheet_name: str,
    df: pd.DataFrame,
) -> None:
    LOGGER.info(
        "Uploading %s rows x %s columns to worksheet '%s' in sheet %s",
        len(df),
        len(df.columns),
        worksheet_name,
        sheet_key,
    )
    worksheet = client.open_by_key(sheet_key).worksheet(worksheet_name)
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    LOGGER.info("Upload complete for worksheet '%s'", worksheet_name)


def run(config: Config) -> None:
    validate_config(config)
    client = authorize_gspread(config.creds_file)

    attendance_df = fetch_worksheet_as_dataframe(
        client,
        config.delivery_sheet_key,
        config.delivery_worksheet_name,
    )
    activity_df = build_inhouse_activity_dataframe(attendance_df)

    report_df = fetch_worksheet_as_dataframe(
        client,
        config.report_sheet_key,
        config.report_worksheet_name,
    )
    report_metrics_df = build_report_metrics_dataframe(report_df, config.report_year_filter)

    final_breakdown_df = build_final_breakdown_dataframe(report_metrics_df, activity_df)
    summary_df = build_summary_dataframe(final_breakdown_df)

    upload_dataframe(
        client,
        config.output_sheet_key,
        config.output_breakdown_worksheet,
        final_breakdown_df,
    )
    upload_dataframe(
        client,
        config.output_sheet_key,
        config.output_summary_worksheet,
        summary_df,
    )

    LOGGER.info("Process completed successfully")


def main() -> int:
    config = parse_args()
    setup_logging(config.log_level)

    try:
        run(config)
        return 0
    except (ConfigurationError, DataValidationError) as exc:
        LOGGER.error("Validation failed: %s", exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Unhandled error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())