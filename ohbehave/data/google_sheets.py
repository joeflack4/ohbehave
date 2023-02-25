"""Get data from google sheets

# Docs
Quickstart:
https://developers.google.com/sheets/api/quickstart/python
Project creation and mgmt:
https://developers.google.com/workspace/guides/create-project
Create creds:
https://developers.google.com/workspace/guides/create-credentials

# Setup
Google cloud project console:
https://console.cloud.google.com/apis/credentials/oauthclient/299107039403-jm7n7m3s9u771dnec1kncsllgoiv8p5a.apps.googleusercontent.com?project=ohbehave

# Data sources
Sheet of interest:
https://docs.google.com/spreadsheets/d/1dOFbfTFReRhJUxjj8TdLvsyOnBJ_WlPvpqXwj48WgVU/edit#gid=1971461617
"""

import json
import os
import sys
from typing import List, Dict
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime_str

from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
from pandas import DataFrame

from ohbehave.config import ENV_DIR, CACHE_DIR, ASSUMPTIONS

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
SAMPLE_SPREADSHEET_ID = '1dOFbfTFReRhJUxjj8TdLvsyOnBJ_WlPvpqXwj48WgVU'
SAMPLE_RANGE_NAME = 'Form Responses 1!A1:L'
TOKEN_PATH = os.path.join(ENV_DIR, 'token.json')
CREDS_PATH = os.path.join(ENV_DIR, 'credentials.json')
SRC_SHEET_FIELDS = {
    # Native
    'timestamp': 'Timestamp',
    'event': 'A) Report event (今)',
    'start_stop': 'Is now the stop or start time?',
    'event_past': 'B) Report event (別時)',
    'start_stop_past': 'Retro: stop or start time?',
    'timestamp_past_time': 'Retro: Time',
    'timestamp_past_date': 'Retro: Date',
    'comments': 'comments',
    # Added here
    'timestamp_past': 'Retro.Timestamp'
}
FLD = SRC_SHEET_FIELDS
GSHEET_JSON_CACHE_PATH = os.path.join(CACHE_DIR, 'data.json')
FUTURE_RETRO_THRESH_HRS = ASSUMPTIONS['maxHoursRetroWasUsedToActuallyReportFutureEvent']  # dk if im using
LATEST_SLEEP_HR = ASSUMPTIONS['latestExpectedSleepHour']


def _get_and_use_new_token():
    """Get new api token"""
    flow = InstalledAppFlow.from_client_secrets_file(
        CREDS_PATH, SCOPES)
    # creds = flow.run_local_server(port=0)
    creds = flow.run_local_server(port=54553)
    # Save the credentials for the next run
    with open(TOKEN_PATH, 'w') as token:
        token.write(creds.to_json())


def _get_sheets_live() -> Dict:
    """Get sheets from online source"""
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        try:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                _get_and_use_new_token()
        except RefreshError:
            _get_and_use_new_token()

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result: Dict = sheet.values().get(
        spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=SAMPLE_RANGE_NAME).execute()

    return result


def _get_sheets_cache(path=GSHEET_JSON_CACHE_PATH) -> Dict:
    """Get sheets from local cache"""
    try:
        with open(path) as f:
            result: Dict = json.load(f)
        return result
    except FileNotFoundError:
        return {}


def retro_date_fillna(row):
    """Make retro timestmap
    todo: for future dates/etc, would be better if I could use minutes too and not just hour calcs"""
    retro_date = row['Retro: Date']
    retro_time = row['Retro: Time']
    reported_hr: int = row['Timestamp'].hour
    if retro_date:
        return retro_date
    elif not retro_time:
        return ''  # handles edge case where neither was filled out

    retro_minus_reported_hrs: int = retro_time.hour - reported_hr
    reported_betw_12am_and_latest_sleep: bool = reported_hr < LATEST_SLEEP_HR.hour
    retro_betw_12am_and_latest_sleep: bool = retro_time.hour < LATEST_SLEEP_HR.hour

    # Cases: Reporting 'retro' time in the future
    # reported_in_future: 2nd 'or' clause: handles retro_time.hour >12am and reported_hr <12am,
    # ...e.g. if FUTURE_RETRO_THRESH_HRS is 2, -22 hours would be considered a future event.
    reported_in_future: bool = \
        retro_minus_reported_hrs <= FUTURE_RETRO_THRESH_HRS or \
        retro_minus_reported_hrs <= FUTURE_RETRO_THRESH_HRS - 24
    if reported_in_future:
        # Example case: Current time 11am, retro time 1am
        # - not considered future event because not within `FUTURE_RETRO_THRESH_HRS`
        # Example case: Current time 11pm, retro time 2am
        # - not considered future event because not within `FUTURE_RETRO_THRESH_HRS`
        # Example case: Current time 1am, retro time 1:30am
        if reported_betw_12am_and_latest_sleep and retro_betw_12am_and_latest_sleep:
            return row['Timestamp'].date()
        # Example case: Current time 11pm, retro time 11:30pm
        elif not reported_betw_12am_and_latest_sleep and not retro_betw_12am_and_latest_sleep:
            return row['Timestamp'].date()
        # Example case: Current time 11pm, retro time 1am
        elif not reported_betw_12am_and_latest_sleep and retro_betw_12am_and_latest_sleep:
            return row['Timestamp'].date() + timedelta(days=1)
        else:
            print("Failed to anticipate case for imputing retro date. Row: ", dict(row), file=sys.stderr)
            return ''

    # Cases: Retro time in past as expected
    # Example case: Current time 11pm, retro time 10:30pm
    if not reported_betw_12am_and_latest_sleep and not retro_betw_12am_and_latest_sleep:
        return row['Timestamp'].date()
    # Example case: Current time 3am, retro time 2am
    # Example case: Current time 1am, retro time 5am
    # todo: 2nd case: fuzzy as to whether or not I would have reported thisa future or past, even though it doesn't
    #  meet FUTURE_RETRO_THRESH_HRS. If I want to inteperet this as past, I would need more logic to catch the case
    #  and chagne return to: return row['Timestamp'].date() - timedelta(days=1)
    #  For now, this case will be handled as in the above clause; considered the same day; a future event.
    elif reported_betw_12am_and_latest_sleep and retro_betw_12am_and_latest_sleep:
        return row['Timestamp'].date()
    # Example case: Current time 3am, retro time 10:30pm
    # Example case: Current time 3am, retro time 11am
    elif reported_betw_12am_and_latest_sleep and not retro_betw_12am_and_latest_sleep:
        return row['Timestamp'].date() - timedelta(days=1)
    # Example case: Current time 11pm, retro time 5am
    # todo: fuzzy as to whether or not I would have reported this as a future event or past, even though it doesn't
    #  meet FUTURE_RETRO_THRESH_HRS. If I want to inteperet this as future, I would change to:
    #  return row['Timestamp'].date() + timedelta(days=1)
    elif not reported_betw_12am_and_latest_sleep and retro_betw_12am_and_latest_sleep:
        return row['Timestamp'].date()
    else:
        print("Failed to anticipate case for imputing retro date. Row: ", dict(row), file=sys.stderr)
        return ''


def get_sheets_data_raw(
    cache_threshold_datetime: datetime = datetime.now() - timedelta(days=7), ignore_gsheets_cache=False
) -> pd.DataFrame:
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    The default cache date is a week ago. So if the last reported data in the
    cached file is less than 7 days ago, cache is used. Else, it loads live data
    and overwrites cache.
    """
    # Get data
    result: Dict = {}
    if cache_threshold_datetime and not ignore_gsheets_cache:
        cached: Dict = _get_sheets_cache()
        last_timestamp = parse_datetime_str(cached['values'][-1][0]) if cached else None
        if last_timestamp and last_timestamp > cache_threshold_datetime:
            result = cached
    if not result:
        result = _get_sheets_live()
        with open(GSHEET_JSON_CACHE_PATH, 'w') as fp:
            json.dump(result, fp)

    # Vars
    values: List[List[str]] = result.get('values', [])
    header = values[0]
    values = values[1:]
    df: DataFrame = pd.DataFrame(values, columns=header).fillna('')

    # Convert strings to datetime/date/time objects
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], infer_datetime_format=True)
    df['Retro: Date'] = df['Retro: Date'].apply(lambda x: parse_datetime_str(x).date() if x else '')
    df['Retro: Time'] = df['Retro: Time'].apply(lambda x: parse_datetime_str(x).time() if x else '')

    return df


def gsheets_datetime_imputations(df: pd.DataFrame) -> pd.DataFrame:
    """Imputes a lot of datetime data"""
    # Retro.Timestamp
    df['Retro: Date'] = df.apply(retro_date_fillna, axis=1)
    df['Retro.Timestamp'] = df.apply(
        lambda x: parse_datetime_str(str(x['Retro: Date']) + ' ' + str(x['Retro: Time']))
        if x['Retro: Date'] and x['Retro: Time'] else '', axis=1)
    return df


def get_sheets_data(
    cache_threshold_datetime: datetime = datetime.now() - timedelta(days=7), ignore_gsheets_cache=False
):
    """Gets raw data and does some datetime imputation"""
    df = get_sheets_data_raw(cache_threshold_datetime, ignore_gsheets_cache)
    df = gsheets_datetime_imputations(df)
    return df


if __name__ == '__main__':
    get_sheets_data()
