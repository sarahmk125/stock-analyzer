import pandas as pd
import json
import logging

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime
import app.secrets as secrets  # CHANGE LATER?


class GoogleSheetIntegration(object):

    def __init__(self):
        # Handles all communicaiton with db
        self.service = self._get_service()

    def _get_service(self):
        # Authenticate
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_string = secrets.SHEETS_CREDENTIALS
        creds_json = json.loads(creds_string)
        creds = Credentials.from_service_account_info(creds_json)
        service = build('sheets', 'v4', credentials=creds)
        return service

    def _get_google_sheet(self, spreadsheet_id, range_name):
        """ Retrieve sheet data using OAuth credentials and Google Python API. """
        # Call the Sheets API
        gsheet = self.service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        print(f"[GoogleSheet] Fetched google sheet {range_name} in spreadsheet ID {spreadsheet_id}.")
        return gsheet

    def _get_google_workbook(self, spreadsheet_id):
        gworkbook = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        print(f"[GoogleSheet] Fetched google workbook with spreadsheet ID {spreadsheet_id}.")
        return gworkbook

    def _gsheet_to_df(self, gsheet, header):
        """ Converts Google sheet data to a Pandas DataFrame.
        Note: This script assumes that your data contains a header file on the first row!
        Also note that the Google API returns 'none' from empty cells - in order for the code
        below to work, you'll need to make sure your sheet doesn't contain empty cells,
        or update the code to account for such instances.
        Updated Note: Logic added to handle empty cells at end by adding dummy empty strings
        to list.
        """
        # Check if the sheet is empty
        if not gsheet.get('values', []):
            print(f"[GoogleSheet] Found no data in sheet, continuing...")
            return pd.DataFrame()

        if not header:
            header = gsheet.get('values', [])[0]   # Assumes first line is header! Note, we are not using this for formatting reasons.
        values = gsheet.get('values', [])[1:]  # Everything else is data.

        # Empty column values at end; nothing is returned, values is just changed
        adjust_empty_values = [[row.append('') for i in range(len(header) - len(row))] for row in values]
        df = pd.DataFrame(values, columns=header)
        return df

    def _columns_to_sheet(self, spreadsheet, worksheet, columns):
        # Transfor columns into list of lists
        full_list = [columns]
        data = [
            {
                'range': worksheet,
                'values': full_list
            },
        ]
        body = {
            'valueInputOption': 'RAW',
            'data': data
        }

        result = self.service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet, body=body).execute()
        print(f"[GoogleSheet] Inserted header columns into sheet id {spreadsheet}.")

    def _strip_col_timestamp(self, series):
        series = series.apply(lambda x: datetime.replace(x, tzinfo=None))
        return series

    def sheet_to_df(self, spreadsheet, worksheet, header=None, clear=False, readd_cols=None):
        # Get google sheet data into df to manipulate
        gsheet = self._get_google_sheet(spreadsheet, worksheet)
        sheet_df = self._gsheet_to_df(gsheet, header)

        # Check for empty sheet
        if sheet_df.empty:
            print(f"[GoogleSheet] Found no data in sheet {spreadsheet}, continuing...")
            return pd.DataFrame()

        return sheet_df

    def df_to_sheet(self, spreadsheet, worksheet, df, column_order=None, value_input_option='RAW', rename_columns=False):
        # Check for empty df
        if df.empty:
            print("[GoogleSheet] Passed empty dataframe to df_to_sheet, continuing...")
            return

        # First, convert all dates in df to strings.
        new_df = df.apply(lambda col: col.astype(str), axis=1)
        new_df = new_df.replace('None', '')
        new_df = new_df.replace('NaN', '')
        new_df = new_df.replace('nan', '')
        new_df = new_df.replace('NaT', '')
        new_df = new_df.replace('nat', '')

        # Re order columns if necessary
        if column_order:
            # First, change the column names if they are not the same to column_order, treating as naming columns
            if rename_columns:
                new_df.columns = column_order

            # Now, change the column order
            new_df = new_df[column_order]

        # Add header to values list
        try:
            values = new_df.values.tolist()
            full_list = [list(new_df.columns)]
            full_list.extend(values)
            data = [
                {
                    'range': worksheet,
                    'values': full_list
                },
            ]
            body = {
                'valueInputOption': value_input_option,
                'data': data
            }

            # Remove all values from sheet, then add header row, then add new values
            # NOTE: this does not remove formatting, so that's gnarly.
            result = self.service.spreadsheets().values().clear(spreadsheetId=spreadsheet, range=worksheet).execute()
            result = self.service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet, body=body).execute()
            print(f"[GoogleSheet] Inserted {len(values)} rows into sheet id {spreadsheet}.")
        except TypeError:
            # Sometimes it has to be converted to a string, specifically the QB stuff. Doesn't work with floats (None).
            values = new_df.values.astype(str).tolist()
            full_list = [list(new_df.columns)]
            full_list.extend(values)
            data = [
                {
                    'range': worksheet,
                    'values': full_list
                },
            ]
            body = {
                'valueInputOption': 'RAW',
                'data': data
            }

            # Remove all values from sheet, then add header row, then add new values
            # NOTE: this does not remove formatting, so that's gnarly.
            result = self.service.spreadsheets().values().clear(spreadsheetId=spreadsheet, range=worksheet).execute()
            result = self.service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet, body=body).execute()
            print(f"[GoogleSheet] Inserted {len(values)} rows into sheet id {spreadsheet}.")

        return 'Completed'