#!/usr/bin/env python3
"""
Google Spreadsheet MCP Server

A Model Context Protocol (MCP) server for interacting with Google Sheets,
built with OpenMCP framework for Dedalus platform compatibility.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from typing import Any, Dict, List, Optional

from dedalus_mcp import MCPServer, tool

# Google API imports
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.auth

# Suppress verbose logs for clean output
for logger_name in ("mcp", "httpx", "uvicorn", "uvicorn.access", "uvicorn.error"):
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

# Constants
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
CREDENTIALS_CONFIG = os.environ.get("CREDENTIALS_CONFIG")
TOKEN_PATH = os.environ.get("TOKEN_PATH", "token.json")
CREDENTIALS_PATH = os.environ.get("CREDENTIALS_PATH", "credentials.json")
SERVICE_ACCOUNT_PATH = os.environ.get("SERVICE_ACCOUNT_PATH", "service_account.json")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

# Global services (initialized at startup)
_sheets_service = None
_drive_service = None
_folder_id = None


def _get_sheets_service():
    """Get the Google Sheets service instance."""
    if _sheets_service is None:
        raise RuntimeError("Sheets service not initialized. Call initialize_services() first.")
    return _sheets_service


def _get_drive_service():
    """Get the Google Drive service instance."""
    if _drive_service is None:
        raise RuntimeError("Drive service not initialized. Call initialize_services() first.")
    return _drive_service


def _get_folder_id() -> Optional[str]:
    """Get the configured default folder ID."""
    return _folder_id


def initialize_services():
    """Initialize Google API services with authentication."""
    global _sheets_service, _drive_service, _folder_id

    creds = None

    # Method 1: Base64-encoded credentials from environment
    if CREDENTIALS_CONFIG:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(base64.b64decode(CREDENTIALS_CONFIG)), scopes=SCOPES
        )
        print("Using base64-encoded service account credentials")

    # Method 2: Service account file
    if not creds and SERVICE_ACCOUNT_PATH and os.path.exists(SERVICE_ACCOUNT_PATH):
        try:
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_PATH, scopes=SCOPES
            )
            print("Using service account authentication")
        except Exception as e:
            print(f"Error using service account authentication: {e}")
            creds = None

    # Method 3: OAuth flow
    if not creds:
        print("Trying OAuth authentication flow")
        if os.path.exists(TOKEN_PATH):
            with open(TOKEN_PATH, "r") as token:
                creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    print("Attempting to refresh expired token...")
                    creds.refresh(Request())
                    print("Token refreshed successfully")
                    with open(TOKEN_PATH, "w") as token:
                        token.write(creds.to_json())
                except Exception as refresh_error:
                    print(f"Token refresh failed: {refresh_error}")
                    creds = None

            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        CREDENTIALS_PATH, SCOPES
                    )
                    creds = flow.run_local_server(port=0)
                    with open(TOKEN_PATH, "w") as token:
                        token.write(creds.to_json())
                    print("Successfully authenticated using OAuth flow")
                except Exception as e:
                    print(f"Error with OAuth flow: {e}")
                    creds = None

    # Method 4: Application Default Credentials
    if not creds:
        try:
            print("Attempting to use Application Default Credentials (ADC)")
            creds, project = google.auth.default(scopes=SCOPES)
            print(f"Successfully authenticated using ADC for project: {project}")
        except Exception as e:
            print(f"Error using Application Default Credentials: {e}")
            raise Exception(
                "All authentication methods failed. Please configure credentials."
            )

    # Build services
    _sheets_service = build("sheets", "v4", credentials=creds)
    _drive_service = build("drive", "v3", credentials=creds)
    _folder_id = DRIVE_FOLDER_ID if DRIVE_FOLDER_ID else None

    print(f"Working with Google Drive folder ID: {_folder_id or 'Not specified'}")


# Initialize the MCP server
server = MCPServer("google-sheets")


with server.binding():

    @tool(description="Get data from a specific sheet in a Google Spreadsheet")
    def get_sheet_data(
        spreadsheet_id: str,
        sheet: str,
        range: Optional[str] = None,
        include_grid_data: bool = False,
    ) -> Dict[str, Any]:
        """Get data from a specific sheet in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            sheet: The name of the sheet
            range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all data.
            include_grid_data: If True, includes cell formatting metadata. Default False for efficiency.

        Returns:
            Grid data structure with values or full metadata from Google Sheets API
        """
        sheets_service = _get_sheets_service()

        if range:
            full_range = f"{sheet}!{range}"
        else:
            full_range = sheet

        if include_grid_data:
            result = (
                sheets_service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id, ranges=[full_range], includeGridData=True)
                .execute()
            )
        else:
            values_result = (
                sheets_service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=full_range)
                .execute()
            )
            result = {
                "spreadsheetId": spreadsheet_id,
                "valueRanges": [
                    {"range": full_range, "values": values_result.get("values", [])}
                ],
            }

        return result

    @tool(description="Get formulas from a specific sheet in a Google Spreadsheet")
    def get_sheet_formulas(
        spreadsheet_id: str, sheet: str, range: Optional[str] = None
    ) -> List[List[Any]]:
        """Get formulas from a specific sheet in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            sheet: The name of the sheet
            range: Optional cell range in A1 notation. If not provided, gets all formulas.

        Returns:
            A 2D array of the sheet formulas
        """
        sheets_service = _get_sheets_service()

        if range:
            full_range = f"{sheet}!{range}"
        else:
            full_range = sheet

        result = (
            sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=full_range, valueRenderOption="FORMULA")
            .execute()
        )

        return result.get("values", [])

    @tool(description="Update cells in a Google Spreadsheet")
    def update_cells(
        spreadsheet_id: str, sheet: str, range: str, data: List[List[Any]]
    ) -> Dict[str, Any]:
        """Update cells in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            sheet: The name of the sheet
            range: Cell range in A1 notation (e.g., 'A1:C10')
            data: 2D array of values to update

        Returns:
            Result of the update operation
        """
        sheets_service = _get_sheets_service()
        full_range = f"{sheet}!{range}"

        result = (
            sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=full_range,
                valueInputOption="USER_ENTERED",
                body={"values": data},
            )
            .execute()
        )

        return result

    @tool(description="Batch update multiple ranges in a Google Spreadsheet")
    def batch_update_cells(
        spreadsheet_id: str, sheet: str, ranges: Dict[str, List[List[Any]]]
    ) -> Dict[str, Any]:
        """Batch update multiple ranges in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            sheet: The name of the sheet
            ranges: Dictionary mapping range strings to 2D arrays of values
                   e.g., {'A1:B2': [[1, 2], [3, 4]], 'D1:E2': [['a', 'b'], ['c', 'd']]}

        Returns:
            Result of the batch update operation
        """
        sheets_service = _get_sheets_service()

        data = []
        for range_str, values in ranges.items():
            full_range = f"{sheet}!{range_str}"
            data.append({"range": full_range, "values": values})

        result = (
            sheets_service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": data},
            )
            .execute()
        )

        return result

    @tool(description="Add rows to a sheet in a Google Spreadsheet")
    def add_rows(
        spreadsheet_id: str, sheet: str, count: int, start_row: Optional[int] = None
    ) -> Dict[str, Any]:
        """Add rows to a sheet in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            sheet: The name of the sheet
            count: Number of rows to add
            start_row: 0-based row index to start adding. If not provided, adds at the beginning.

        Returns:
            Result of the operation
        """
        sheets_service = _get_sheets_service()

        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None

        for s in spreadsheet["sheets"]:
            if s["properties"]["title"] == sheet:
                sheet_id = s["properties"]["sheetId"]
                break

        if sheet_id is None:
            return {"error": f"Sheet '{sheet}' not found"}

        request_body = {
            "requests": [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": start_row if start_row is not None else 0,
                            "endIndex": (start_row if start_row is not None else 0) + count,
                        },
                        "inheritFromBefore": start_row is not None and start_row > 0,
                    }
                }
            ]
        }

        result = (
            sheets_service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        return result

    @tool(description="Add columns to a sheet in a Google Spreadsheet")
    def add_columns(
        spreadsheet_id: str, sheet: str, count: int, start_column: Optional[int] = None
    ) -> Dict[str, Any]:
        """Add columns to a sheet in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            sheet: The name of the sheet
            count: Number of columns to add
            start_column: 0-based column index to start adding. If not provided, adds at the beginning.

        Returns:
            Result of the operation
        """
        sheets_service = _get_sheets_service()

        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None

        for s in spreadsheet["sheets"]:
            if s["properties"]["title"] == sheet:
                sheet_id = s["properties"]["sheetId"]
                break

        if sheet_id is None:
            return {"error": f"Sheet '{sheet}' not found"}

        request_body = {
            "requests": [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": start_column if start_column is not None else 0,
                            "endIndex": (start_column if start_column is not None else 0) + count,
                        },
                        "inheritFromBefore": start_column is not None and start_column > 0,
                    }
                }
            ]
        }

        result = (
            sheets_service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        return result

    @tool(description="List all sheets in a Google Spreadsheet")
    def list_sheets(spreadsheet_id: str) -> List[str]:
        """List all sheets in a Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)

        Returns:
            List of sheet names
        """
        sheets_service = _get_sheets_service()
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return [sheet["properties"]["title"] for sheet in spreadsheet["sheets"]]

    @tool(description="Copy a sheet from one spreadsheet to another")
    def copy_sheet(
        src_spreadsheet: str, src_sheet: str, dst_spreadsheet: str, dst_sheet: str
    ) -> Dict[str, Any]:
        """Copy a sheet from one spreadsheet to another.

        Args:
            src_spreadsheet: Source spreadsheet ID
            src_sheet: Source sheet name
            dst_spreadsheet: Destination spreadsheet ID
            dst_sheet: Destination sheet name

        Returns:
            Result of the operation
        """
        sheets_service = _get_sheets_service()

        src = sheets_service.spreadsheets().get(spreadsheetId=src_spreadsheet).execute()
        src_sheet_id = None

        for s in src["sheets"]:
            if s["properties"]["title"] == src_sheet:
                src_sheet_id = s["properties"]["sheetId"]
                break

        if src_sheet_id is None:
            return {"error": f"Source sheet '{src_sheet}' not found"}

        copy_result = (
            sheets_service.spreadsheets()
            .sheets()
            .copyTo(
                spreadsheetId=src_spreadsheet,
                sheetId=src_sheet_id,
                body={"destinationSpreadsheetId": dst_spreadsheet},
            )
            .execute()
        )

        if "title" in copy_result and copy_result["title"] != dst_sheet:
            copy_sheet_id = copy_result["sheetId"]
            rename_request = {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": copy_sheet_id, "title": dst_sheet},
                            "fields": "title",
                        }
                    }
                ]
            }

            rename_result = (
                sheets_service.spreadsheets()
                .batchUpdate(spreadsheetId=dst_spreadsheet, body=rename_request)
                .execute()
            )

            return {"copy": copy_result, "rename": rename_result}

        return {"copy": copy_result}

    @tool(description="Rename a sheet in a Google Spreadsheet")
    def rename_sheet(spreadsheet: str, sheet: str, new_name: str) -> Dict[str, Any]:
        """Rename a sheet in a Google Spreadsheet.

        Args:
            spreadsheet: Spreadsheet ID
            sheet: Current sheet name
            new_name: New sheet name

        Returns:
            Result of the operation
        """
        sheets_service = _get_sheets_service()

        spreadsheet_data = (
            sheets_service.spreadsheets().get(spreadsheetId=spreadsheet).execute()
        )
        sheet_id = None

        for s in spreadsheet_data["sheets"]:
            if s["properties"]["title"] == sheet:
                sheet_id = s["properties"]["sheetId"]
                break

        if sheet_id is None:
            return {"error": f"Sheet '{sheet}' not found"}

        request_body = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "title": new_name},
                        "fields": "title",
                    }
                }
            ]
        }

        result = (
            sheets_service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet, body=request_body)
            .execute()
        )

        return result

    @tool(description="Get data from multiple specific ranges in Google Spreadsheets")
    def get_multiple_sheet_data(queries: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Get data from multiple specific ranges in Google Spreadsheets.

        Args:
            queries: A list of dictionaries with 'spreadsheet_id', 'sheet', and 'range' keys.
                    Example: [{'spreadsheet_id': 'abc', 'sheet': 'Sheet1', 'range': 'A1:B5'}]

        Returns:
            A list of dictionaries with the query parameters and fetched 'data' or 'error'
        """
        sheets_service = _get_sheets_service()
        results = []

        for query in queries:
            spreadsheet_id = query.get("spreadsheet_id")
            sheet = query.get("sheet")
            range_str = query.get("range")

            if not all([spreadsheet_id, sheet, range_str]):
                results.append(
                    {**query, "error": "Missing required keys (spreadsheet_id, sheet, range)"}
                )
                continue

            try:
                full_range = f"{sheet}!{range_str}"
                result = (
                    sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=spreadsheet_id, range=full_range)
                    .execute()
                )
                values = result.get("values", [])
                results.append({**query, "data": values})

            except Exception as e:
                results.append({**query, "error": str(e)})

        return results

    @tool(description="Get a summary of multiple Google Spreadsheets")
    def get_multiple_spreadsheet_summary(
        spreadsheet_ids: List[str], rows_to_fetch: int = 5
    ) -> List[Dict[str, Any]]:
        """Get a summary of multiple Google Spreadsheets including headers and first rows.

        Args:
            spreadsheet_ids: A list of spreadsheet IDs to summarize
            rows_to_fetch: The number of rows (including header) to fetch for the summary (default: 5)

        Returns:
            A list of dictionaries with spreadsheet summaries
        """
        sheets_service = _get_sheets_service()
        summaries = []

        for spreadsheet_id in spreadsheet_ids:
            summary_data = {
                "spreadsheet_id": spreadsheet_id,
                "title": None,
                "sheets": [],
                "error": None,
            }
            try:
                spreadsheet = (
                    sheets_service.spreadsheets()
                    .get(
                        spreadsheetId=spreadsheet_id,
                        fields="properties.title,sheets(properties(title,sheetId))",
                    )
                    .execute()
                )

                summary_data["title"] = spreadsheet.get("properties", {}).get(
                    "title", "Unknown Title"
                )

                sheet_summaries = []
                for sheet in spreadsheet.get("sheets", []):
                    sheet_title = sheet.get("properties", {}).get("title")
                    sheet_id = sheet.get("properties", {}).get("sheetId")
                    sheet_summary = {
                        "title": sheet_title,
                        "sheet_id": sheet_id,
                        "headers": [],
                        "first_rows": [],
                        "error": None,
                    }

                    if not sheet_title:
                        sheet_summary["error"] = "Sheet title not found"
                        sheet_summaries.append(sheet_summary)
                        continue

                    try:
                        max_row = max(1, rows_to_fetch)
                        range_to_get = f"{sheet_title}!A1:{max_row}"

                        result = (
                            sheets_service.spreadsheets()
                            .values()
                            .get(spreadsheetId=spreadsheet_id, range=range_to_get)
                            .execute()
                        )

                        values = result.get("values", [])

                        if values:
                            sheet_summary["headers"] = values[0]
                            if len(values) > 1:
                                sheet_summary["first_rows"] = values[1:max_row]
                        else:
                            sheet_summary["headers"] = []
                            sheet_summary["first_rows"] = []

                    except Exception as sheet_e:
                        sheet_summary["error"] = (
                            f"Error fetching data for sheet {sheet_title}: {sheet_e}"
                        )

                    sheet_summaries.append(sheet_summary)

                summary_data["sheets"] = sheet_summaries

            except Exception as e:
                summary_data["error"] = f"Error fetching spreadsheet {spreadsheet_id}: {e}"

            summaries.append(summary_data)

        return summaries

    @tool(description="Create a new Google Spreadsheet")
    def create_spreadsheet(
        title: str, folder_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new Google Spreadsheet.

        Args:
            title: The title of the new spreadsheet
            folder_id: Optional Google Drive folder ID. If not provided, uses configured default.

        Returns:
            Information about the newly created spreadsheet including its ID
        """
        drive_service = _get_drive_service()
        target_folder_id = folder_id or _get_folder_id()

        file_body = {
            "name": title,
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        if target_folder_id:
            file_body["parents"] = [target_folder_id]

        spreadsheet = (
            drive_service.files()
            .create(supportsAllDrives=True, body=file_body, fields="id, name, parents")
            .execute()
        )

        spreadsheet_id = spreadsheet.get("id")
        parents = spreadsheet.get("parents")
        folder_info = f" in folder {target_folder_id}" if target_folder_id else " in root"
        print(f"Spreadsheet created with ID: {spreadsheet_id}{folder_info}")

        return {
            "spreadsheetId": spreadsheet_id,
            "title": spreadsheet.get("name", title),
            "folder": parents[0] if parents else "root",
        }

    @tool(description="Create a new sheet tab in an existing Google Spreadsheet")
    def create_sheet(spreadsheet_id: str, title: str) -> Dict[str, Any]:
        """Create a new sheet tab in an existing Google Spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet
            title: The title for the new sheet

        Returns:
            Information about the newly created sheet
        """
        sheets_service = _get_sheets_service()

        request_body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}

        result = (
            sheets_service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )

        new_sheet_props = result["replies"][0]["addSheet"]["properties"]

        return {
            "sheetId": new_sheet_props["sheetId"],
            "title": new_sheet_props["title"],
            "index": new_sheet_props.get("index"),
            "spreadsheetId": spreadsheet_id,
        }

    @tool(description="List all spreadsheets in a Google Drive folder")
    def list_spreadsheets(folder_id: Optional[str] = None) -> List[Dict[str, str]]:
        """List all spreadsheets in the specified Google Drive folder.

        Args:
            folder_id: Optional folder ID. If not provided, uses configured default or 'My Drive'.

        Returns:
            List of spreadsheets with their ID and title
        """
        drive_service = _get_drive_service()
        target_folder_id = folder_id or _get_folder_id()

        query = "mimeType='application/vnd.google-apps.spreadsheet'"

        if target_folder_id:
            query += f" and '{target_folder_id}' in parents"
            print(f"Searching for spreadsheets in folder: {target_folder_id}")
        else:
            print("Searching for spreadsheets in 'My Drive'")

        results = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="files(id, name)",
                orderBy="modifiedTime desc",
            )
            .execute()
        )

        spreadsheets = results.get("files", [])

        return [{"id": sheet["id"], "title": sheet["name"]} for sheet in spreadsheets]

    @tool(description="Share a Google Spreadsheet with multiple users")
    def share_spreadsheet(
        spreadsheet_id: str,
        recipients: List[Dict[str, str]],
        send_notification: bool = True,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Share a Google Spreadsheet with multiple users via email.

        Args:
            spreadsheet_id: The ID of the spreadsheet to share
            recipients: List of dicts with 'email_address' and 'role' ('reader', 'commenter', 'writer')
                       Example: [{'email_address': 'user@example.com', 'role': 'writer'}]
            send_notification: Whether to send notification email. Defaults to True.

        Returns:
            Dictionary with 'successes' and 'failures' lists
        """
        drive_service = _get_drive_service()
        successes = []
        failures = []

        for recipient in recipients:
            email_address = recipient.get("email_address")
            role = recipient.get("role", "writer")

            if not email_address:
                failures.append(
                    {"email_address": None, "error": "Missing email_address in recipient entry."}
                )
                continue

            if role not in ["reader", "commenter", "writer"]:
                failures.append(
                    {
                        "email_address": email_address,
                        "error": f"Invalid role '{role}'. Must be 'reader', 'commenter', or 'writer'.",
                    }
                )
                continue

            permission = {"type": "user", "role": role, "emailAddress": email_address}

            try:
                result = (
                    drive_service.permissions()
                    .create(
                        fileId=spreadsheet_id,
                        body=permission,
                        sendNotificationEmail=send_notification,
                        fields="id",
                    )
                    .execute()
                )
                successes.append(
                    {
                        "email_address": email_address,
                        "role": role,
                        "permissionId": result.get("id"),
                    }
                )
            except Exception as e:
                error_details = str(e)
                if hasattr(e, "content"):
                    try:
                        error_content = json.loads(e.content)
                        error_details = error_content.get("error", {}).get(
                            "message", error_details
                        )
                    except json.JSONDecodeError:
                        pass
                failures.append(
                    {"email_address": email_address, "error": f"Failed to share: {error_details}"}
                )

        return {"successes": successes, "failures": failures}

    @tool(description="List all folders in a Google Drive folder")
    def list_folders(parent_folder_id: Optional[str] = None) -> List[Dict[str, str]]:
        """List all folders in the specified Google Drive folder.

        Args:
            parent_folder_id: Optional parent folder ID. If not provided, lists from 'My Drive' root.

        Returns:
            List of folders with their ID, name, and parent information
        """
        drive_service = _get_drive_service()

        query = "mimeType='application/vnd.google-apps.folder'"

        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
            print(f"Searching for folders in parent folder: {parent_folder_id}")
        else:
            query += " and 'root' in parents"
            print("Searching for folders in 'My Drive' root")

        results = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="files(id, name, parents)",
                orderBy="name",
            )
            .execute()
        )

        folders = results.get("files", [])

        return [
            {
                "id": folder["id"],
                "name": folder["name"],
                "parent": folder.get("parents", ["root"])[0] if folder.get("parents") else "root",
            }
            for folder in folders
        ]

    @tool(description="Execute a batch update on a Google Spreadsheet using the full batchUpdate endpoint")
    def batch_update(
        spreadsheet_id: str, requests: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute a batch update on a Google Spreadsheet.

        This provides access to all batchUpdate operations including adding sheets,
        updating properties, inserting/deleting dimensions, formatting, and more.

        Args:
            spreadsheet_id: The ID of the spreadsheet (found in the URL)
            requests: A list of request objects. Common operations include:
                     - addSheet: Add a new sheet
                     - updateSheetProperties: Update sheet properties
                     - insertDimension: Insert rows or columns
                     - deleteDimension: Delete rows or columns
                     - updateCells: Update cell values and formatting
                     - updateBorders: Update cell borders
                     - addConditionalFormatRule: Add conditional formatting

        Returns:
            Result of the batch update operation, including replies for each request
        """
        sheets_service = _get_sheets_service()

        if not requests:
            return {"error": "requests list cannot be empty"}

        if not all(isinstance(req, dict) for req in requests):
            return {"error": "Each request must be a dictionary"}

        result = (
            sheets_service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
            .execute()
        )

        return result


async def _async_main() -> None:
    """Start the MCP server on streamable-http transport."""
    # Initialize Google services
    initialize_services()

    print("Starting Google Sheets MCP Server...")
    print("Transport: streamable-http")
    print(
        "Available tools: get_sheet_data, get_sheet_formulas, update_cells, "
        "batch_update_cells, add_rows, add_columns, list_sheets, copy_sheet, "
        "rename_sheet, get_multiple_sheet_data, get_multiple_spreadsheet_summary, "
        "create_spreadsheet, create_sheet, list_spreadsheets, share_spreadsheet, "
        "list_folders, batch_update"
    )
    print()

    await server.serve(transport="streamable-http", verbose=False, log_level="critical")


def main() -> None:
    """Sync entry point for the MCP server."""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
