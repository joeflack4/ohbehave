"""Transform GoogleForms submissiond ata into a 1row=1day DataFrame
"""
import os
# from copy import copy
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime_str
import pickle

import pandas as pd
from numpy import datetime64, ndarray
from pandas import DataFrame, Series

from config import CACHE_DIR
from ohbehave.data.google_sheets import SRC_SHEET_FIELDS as FLD, get_sheets_data


ASSUMPTIONS = {
    'gamingEarliestDailyStart': '9:30:00',  # hh:MM:SS
}


def _weekday_from_date(val: str) -> str:
    """Get weekday from date"""
    week_days = [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
        "Sunday"]
    # Formatting err; so doing parse instead
    # possible_delimiters = ['/', '-', '/', '.']
    # for d in possible_delimiters:
    #     try:
    #         year, month, day = val.split(d)
    #         break
    #     except ValueError:
    #         continue
    # this_date = datetime.date(int(year), int(month), int(day))
    this_date = parse_datetime_str(val)
    week_num = this_date.weekday()
    weekday = week_days[week_num]
    return weekday


# TODO: not performant
# TODO: datetime64 column dtype
def data_by_date(use_cache=False, df_cache_path=os.path.join(CACHE_DIR, 'data_by_date.pickle')) -> DataFrame:
    """Get data by date, 1row=1date"""
    if use_cache:
        file = open(df_cache_path, 'rb')
        df = pickle.load(file)
        file.close()
    else:
        df: DataFrame = get_sheets_data(cache_threshold_datetime=datetime.now() - timedelta(days=7))
        df = transform_by_date(df)  # TODO: not performant
        df['Date'] = df['Date'].astype(datetime64)
        f = open(df_cache_path, 'wb')
        pickle.dump(df, f)
        f.close()
    return df


def transform_by_date(df: DataFrame) -> DataFrame:
    """Transform into new DF where dates:rows ratio is 1:1"""
    # Setup
    column_names = [
        'Date', 'Weekday',
        'GamesFriends.start', 'GamesFriends.stop', 'GamesFriends.pct', 'GamesFriends.tot',  # TODO
        'GamesSolo.start', 'GamesSolo.stop', 'GamesSolo.pct', 'GamesSolo.tot',  # TODO
        'Games.tot',  # TODO
        'Drinks.tot',  # TODO
        'Comments.all'  # TODO
    ]
    df2 = DataFrame(columns=column_names)

    # Set columms: Date, Weekday
    timestamps: Series = df.get(FLD['timestamp'])
    dates: ndarray = timestamps.apply(lambda x: str(x).split()[0]).unique()
    # TODO: fix so that
    df2['Date'] = dates.tolist()
    df2['Weekday'] = df2.get('Date').apply(_weekday_from_date)

    # Set columns: Gaming (pass 1)
    event_indicator_map = {
        'GamesFriends': 'ゲイム、友達と ',
        'GamesSolo': 'ゲイム、自己 '
    }
    for row_i in df.iterrows():
        # Setup
        row = dict(row_i[1])
        event = row[FLD['event']]
        event_past = row[FLD['event_past']]
        start_stop = row[FLD['start_stop']]
        start_stop_past = row[FLD['start_stop_past']]
        timestamp = row[FLD['timestamp']]
        timestamp_past = row[FLD['timestamp_past']]
        date_str = str(timestamp).split()[0]

        # Determine target field & timestamp
        if event == event_indicator_map['GamesFriends']:
            target_field = 'GamesFriends.start' if start_stop == 'Start' else 'GamesFriends.stop'
            target_timestamp = timestamp
        elif event == event_indicator_map['GamesSolo']:
            target_field = 'GamesSolo.start' if start_stop == 'Start' else 'GamesSolo.stop'
            target_timestamp = timestamp
        elif event_past == event_indicator_map['GamesFriends']:
            target_field = 'GamesFriends.start' if start_stop_past == 'Start' else 'GamesFriends.stop'
            target_timestamp = timestamp_past
        elif event_past == event_indicator_map['GamesSolo']:
            target_field = 'GamesSolo.start' if start_stop_past == 'Start' else 'GamesSolo.stop'
            target_timestamp = timestamp_past
        else:
            continue  # Row has no gaming related activity

        # Set values
        target_index = df2.index[df2['Date'] == date_str].tolist()[0]
        df2.at[target_index, target_field] = target_timestamp

    # Set columns: Gaming (pass 2: inference)
    # df3 = copy(df2)
    # for row_i in df2.iterrows():  # TODO
    #     index = row_i[0]
    #     row = dict(row_i[1])
    #     # TODO: timestamp: placeholder!
    #     #  ...should check for gaming[friends/solo][start/stop] times, and for each,
    #     #  ...produce its own date_str based on what's in that field, e.g. row['GamesFriends.start']
    #     timestamp = row['timestamp']
    #     date_str = str(timestamp).split()[0]
    #     date: datetime = parse_datetime_str(date_str)
    #
    #     earliest_time_expected: datetime = parse_datetime_str(str(date) + ' ' + \
    #          str(ASSUMPTIONS['gamingEarliestDailyStart']))
    #     earliest_time_expected_next_day: datetime = \
    #         earliest_time_expected + timedelta(days=1)
    #
    #     # TODO: What was I doing w/ this logic again? Should I ditch it and use something else?
    #     # events_of_day_stamps = \
    #     #     (df[FLD['timestamp']] >= earliest_time_expected) \
    #     #     & (df[FLD['timestamp']] < earliest_time_expected_next_day)
    #
    #     # TODO:
    #     inferred_missing_start_stop_time = ''
    #     # TODO:
    #     target_field = ''  # X.Y where X is GamesSolo or GamesStart; Y is Start or Stop
    #
    #     df3.at[index, target_field] = inferred_missing_start_stop_time

    df3 = df2  # temp
    # Set columns: Drinks
    drink_indicator = '飲み物'
    drinks_field_new = 'Drinks.tot'
    for row_i in df.iterrows():
        # Setup
        row = dict(row_i[1])
        event = row[FLD['event']]
        event_past = row[FLD['event_past']]
        timestamp = row[FLD['timestamp']]
        timestamp_past = row[FLD['timestamp_past']]
        date_str = str(timestamp).split()[0]
        date_str_past = str(timestamp_past).split()[0]

        # Determine if drink & get date
        if event == drink_indicator:
            ref_date_str = date_str
            # If in wee hours of morning past 12am, we consider drink as part of 'previous day'
            use_prev_day_indicator_from: datetime = parse_datetime_str(
                date_str + ' ' + '00:00:00')
            use_prev_day_indicator_to: datetime = parse_datetime_str(
                date_str + ' ' + str(ASSUMPTIONS['gamingEarliestDailyStart']))
            use_prev_day: bool = \
                use_prev_day_indicator_from <= timestamp <= use_prev_day_indicator_to
            if use_prev_day:
                ref_date_str = str(timestamp - timedelta(days=-1)).split()[0]
        elif event_past == drink_indicator:
            ref_date_str = date_str_past
        else:
            continue

        # Set values
        try:
            target_index = df3.index[df3['Date'] == ref_date_str].tolist()[0]
        except IndexError:  # TODO: remove after i fix issue where dates missing
            target_index = df3.index[df3['Date'] == date_str].tolist()[0]
        current_val = df3.at[target_index, drinks_field_new]
        current_val: int = current_val if current_val and not pd.isnull(current_val) and not pd.isna(current_val) else 0
        df3.at[target_index, drinks_field_new] = current_val + 1

    return df3
