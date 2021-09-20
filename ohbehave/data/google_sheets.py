"""Get data from google sheets

# TODO's
# TODO 1: Use cache

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
import os.path
from dateutil.parser import parse as parse_datetime_str
from typing import List

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
from pandas import DataFrame

from ohbehave.config import ENV_DIR


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
SAMPLE_SPREADSHEET_ID = '1dOFbfTFReRhJUxjj8TdLvsyOnBJ_WlPvpqXwj48WgVU'
SAMPLE_RANGE_NAME = 'Form Responses 1!A1:H'
TOKEN_PATH = os.path.join(ENV_DIR, 'token.json')
CREDS_PATH = os.path.join(ENV_DIR, 'credentials.json')

def get_sheets_data() -> pd.DataFrame:
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDS_PATH, SCOPES)
            # creds = flow.run_local_server(port=0)
            creds = flow.run_local_server(port=54553)
        # Save the credentials for the next run
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values: List[List[str]] = result.get('values', [])
    header = values[0]
    values = values[1:]

    df: DataFrame = pd.DataFrame(values, columns=header).fillna('')

    df2 = df  # temp copy for comparing when debugging
    # https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html
    # - Non-infer can take ~0.5sec instead of ~0.001, but maybe better given the
    # ...source data, but I'm not sure.
    df2['Timestamp'] = pd.to_datetime(
        df2['Timestamp'],
        infer_datetime_format=True)

    # TODO: create Retro.Timestamp col based on: parse(Retro:Date (split[0]) +
    #  ' ' + Retro:Time)
    # - 'Retro:Time[0]' is to remove AM/PM and leave behind the hh:MM:SS

    # TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'
    # TODO: thus, need to fill in any missing values for Retro:Date.
    # ...The way I've been entering data, I've been leaving Retro:Date empty,
    # ...since it can be inferred and saves me time when doing data entry. Now,
    # ...to successfuly infer, I will figure that if the given timestamp at the
    # ...point of my entry is an evening hour <midnight, use the date in that
    # ...timestamp. But otherwise, if morning or early afternoon <5pm(?), treat
    # ...as the day before the timestamp (*unless* the retro time reported is
    # ...also in the AM (i.e. before 9am)).
    df2['Retro:Date'] = df2['Retro:Date'].apply(lambda x: x)

    # TODO: This will work correctly after doing previous inference, which will
    # ...fill any None values.
    df2['Retro.Timestamp'] = pd.to_datetime(
        df2.get('Retro:Date') + ' ' + \
        df2.get('Retro:Time').apply(lambda x: x.split()[0])
    )
    return df2


if __name__ == '__main__':
    get_sheets_data()
