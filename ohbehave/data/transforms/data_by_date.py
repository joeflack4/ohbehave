"""Transform GoogleForms submissiond ata into a 1row=1day DataFrame
"""
import os
import sys
from copy import copy
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Union

from dateutil.parser import parse as parse_datetime_str
import pickle

import pandas as pd
from numpy import datetime64
from pandas import DataFrame, Timestamp

from config import ASSUMPTIONS, CACHE_DIR
from ohbehave.data.google_sheets import SRC_SHEET_FIELDS as FLD, get_sheets_data

EVENT_TYPE = tuple[str, Timestamp]
WEEK_DAYS = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
    "Sunday"]
EVENT_INDICATOR_MAP = {
    'GamesFriends': 'ゲイム、友達と ',
    'GamesSolo': 'ゲイム、自己 ',
    'Drink': '飲み物',
    'Sleep': '寝る前',
}


def _timedelta_to_hrs(delta: timedelta, decimal_places=2) -> float:
    """From timedelta obj, get hours"""
    return round(delta.seconds / (60 * 60), decimal_places)


def _weekday_from_date(val: Union[str, date]) -> str:
    """Get weekday from date"""
    if isinstance(val, str):
        val = parse_datetime_str(val)
    week_num = val.weekday()
    weekday = WEEK_DAYS[week_num]
    return weekday


def sleep_summary_stats(input_df: pd.DataFrame) -> pd.DataFrame:
    """Get sleep summary stats from a data_by_date DF"""
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html
    xxx = input_df.groupby(by='Weekday').sum()
    df = input_df
    return df


# TODO: not performant
# TODO: datetime64 column dtype
def data_by_date(
    exclude_gaming_data=False, exclude_alcohol_data=False, exclude_sleep_data=False, ignore_gsheets_cache=False,
    use_cache=False, df_cache_path=os.path.join(CACHE_DIR, 'data_by_date.pickle')
) -> DataFrame:
    """Get data by date, 1row=1date"""
    if use_cache:
        file = open(df_cache_path, 'rb')
        df = pickle.load(file)
        file.close()
    else:
        df: DataFrame = get_sheets_data(datetime.now() - timedelta(days=7), ignore_gsheets_cache)
        df = transform_and_impute(df, exclude_gaming_data, exclude_alcohol_data, exclude_sleep_data)
        df['Date'] = df['Date'].astype(datetime64)
        f = open(df_cache_path, 'wb')
        pickle.dump(df, f)
        f.close()
    return df


# todo: refactor to accept 1 df like other funcs
def transform_gaming(input_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Transform gaming data"""
    # - Gaming data pass 1: raw data
    for row_i in input_df.iterrows():
        # Setup
        row = dict(row_i[1])
        event = row[FLD['event']]
        event_past = row[FLD['event_past']]
        start_stop = row[FLD['start_stop']]
        start_stop_past = row[FLD['start_stop_past']]
        timestamp = row['Timestamp']
        timestamp_past = row[FLD['timestamp_past']]
        date_str = str(timestamp).split()[0]

        # Determine target field & timestamp
        if event == EVENT_INDICATOR_MAP['GamesFriends']:
            target_field = 'GamesFriends.start' if start_stop == 'Start' else 'GamesFriends.stop'
            target_timestamp = timestamp
        elif event == EVENT_INDICATOR_MAP['GamesSolo']:
            target_field = 'GamesSolo.start' if start_stop == 'Start' else 'GamesSolo.stop'
            target_timestamp = timestamp
        elif event_past == EVENT_INDICATOR_MAP['GamesFriends']:
            target_field = 'GamesFriends.start' if start_stop_past == 'Start' else 'GamesFriends.stop'
            target_timestamp = timestamp_past
        elif event_past == EVENT_INDICATOR_MAP['GamesSolo']:
            target_field = 'GamesSolo.start' if start_stop_past == 'Start' else 'GamesSolo.stop'
            target_timestamp = timestamp_past
        else:
            continue  # Row has no gaming related activity

        # Set values
        # target_index = df.index[df['Date'] == date_str].tolist()[0]
        # df.at[target_index, target_field] = target_timestamp
        df.at[date_str, target_field] = target_timestamp

    # - Gaming data pass 2: inference
    # df2 = copy(df)
    # for row_i in df.iterrows():  # TODO
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
    #     #     (input_df['Timestamp'] >= earliest_time_expected) \
    #     #     & (input_df['Timestamp'] < earliest_time_expected_next_day)
    #
    #     # TODO:
    #     inferred_missing_start_stop_time = ''
    #     # TODO:
    #     target_field = ''  # X.Y where X is GamesSolo or GamesStart; Y is Start or Stop
    #
    #     df2.at[index, target_field] = inferred_missing_start_stop_time
    return df


# todo:the Date is now the index, so don't need to look it up at end of loop
def transform_alcohol(input_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Transform gaming data"""
    drinks_field_new = 'Drinks.tot'
    for row_i in input_df.iterrows():
        # Setup
        row = dict(row_i[1])
        event = row[FLD['event']]
        event_past = row[FLD['event_past']]
        timestamp = row['Timestamp']
        timestamp_past = row[FLD['timestamp_past']]
        date_str = str(timestamp).split()[0]
        date_str_past = str(timestamp_past).split()[0]

        # Determine if drink & get date
        if event == EVENT_INDICATOR_MAP['Drink']:
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
        elif event_past == EVENT_INDICATOR_MAP['Drink']:
            ref_date_str = date_str_past
        else:
            continue

        # Set values
        try:
            target_index = df.index[df['Date'] == ref_date_str].tolist()[0]
        except IndexError:  # TODO: remove after i fix issue where dates missing
            target_index = df.index[df['Date'] == date_str].tolist()[0]
        current_val = df.at[target_index, drinks_field_new]
        current_val: int = \
            current_val if current_val and not pd.isnull(current_val) and not pd.isna(current_val) else 0
        df.at[target_index, drinks_field_new] = current_val + 1
    return df


def transform_sleep(input_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Transform gaming data"""
    # todo: if this is more performant, reimplement everything to use this method
    # d = {x['Date']: x for x in df.to_dict(orient='records')}

    # Attempt 2
    # temp comment: fundamentally different than attempt 1. Will probably be different num/nature of iteration
    # Step 1: Collect all sleep events for a each sleep start day
    date_events_map: Dict[date, List[EVENT_TYPE]] = {}
    for _index, row in input_df.iterrows():
        row = dict(row)
        event = row[FLD['event']]
        event_past = row[FLD['event_past']]
        start_stop = row[FLD['start_stop']]
        start_stop_past = row[FLD['start_stop_past']]
        timestamp = row['Timestamp']
        timestamp_past = row[FLD['timestamp_past']]

        # Determine target field & timestamp
        if event == EVENT_INDICATOR_MAP['Sleep']:
            target_field = 'Sleep.start' if start_stop == 'Start' else 'Sleep.wake'
            target_timestamp = timestamp
        elif event_past == EVENT_INDICATOR_MAP['Sleep']:
            target_field = 'Sleep.start' if start_stop_past == 'Start' else 'Sleep.wake'
            target_timestamp = timestamp_past
        else:
            continue  # Row has no related activity
        if not target_timestamp:  # If this happens, I accidentally didn't record retro time
            # As of 2023/02/28: was missing on these dates: 2021-11-26 (2 events), 2022-08-06
            # print('Missing timestamp on date', timestamp.date())
            continue

        # Determine waking day date
        # todo: I see there is a rare edge case in which I could possibly take a nap from, say, 8pm to 10pm. But,
        #  ...I don't think I have a way of distiguisihing this from actually going to sleep for the night at 8pm
        waking_day_date = target_timestamp.date()
        if target_timestamp.hour < ASSUMPTIONS['sleepStartTimeFromWhenNonNap'].hour:
            waking_day_date = target_timestamp.date() - timedelta(days=1)

        # Set values
        date_events_map.setdefault(waking_day_date, []).append((target_field, target_timestamp))

    # Step 2: Parse events
    # TODO: If I don't have a sleep start or a sleep end, find some way to ignore for the day for now
    t0 = datetime.now()
    print('Sleep data: Parsing sleep segments: start')
    for waking_date, events in date_events_map.items():
        dict_row = df.loc[[waking_date]].to_dict(orient='records')[0]
        events = sorted(events, key=lambda x: x[1])  # sorts by timestamp just in case

        # Get all sleep segements (main sleep event & naps)
        # - This can involve multiple Sleep.start before an eventual Sleep.wake
        sleep_segments: List[List[EVENT_TYPE]] = []
        current_sleep_segment: List[EVENT_TYPE] = []
        for event in events:
            current_sleep_segment.append(event)
            if event[0] == 'Sleep.wake':
                sleep_segments.append(copy(current_sleep_segment))
                current_sleep_segment.clear()

        if not sleep_segments:  # todo: in future I want to explicitly impute
            continue

        dict_row['Sleep.interruptions.natural'] = 0
        dict_row['Sleep.interruptions.alarm'] = 0
        dict_row['Sleep.interruptions.tot'] = 0
        dict_row['Sleep.start.timestamp'] = sleep_segments[0][0][1]
        dict_row['Sleep.end.mainSegement.timestamp'] = sleep_segments[0][-1][1]
        dict_row['Sleep.end.allSegments.timestamp'] = sleep_segments[-1][-1][1]
        dict_row['Sleep.duration.hrs'] = 0

        # TODO: from segments, calculate all this stuff and assign to dict_row
        # Calc duration & time taken to fall asleep (non naps)
        for i, seg in enumerate(sleep_segments):
            if len(seg) < 2:  # temp?
                print('Missing sleep start on: ', waking_date)
                continue

            # Sleep duration
            est_post_report_sleep_delay = ASSUMPTIONS['avgTimeAfterLoggingToFirstSleepIfLogged1x'] \
                if len(seg) <= 2 else ASSUMPTIONS['avgTimeAfterLoggingToFirstSleepIfLogged2+']
            last_reported_sleep_start = seg[-2][1]
            est_actual_sleep_start = last_reported_sleep_start + est_post_report_sleep_delay
            sleep_end = seg[-1][1]
            duration_hrs = _timedelta_to_hrs(sleep_end - est_actual_sleep_start)
            if duration_hrs < 0:  # temp?
                print(f'Unexpected negative sleep duration: {duration_hrs}: ', seg)
            else:
                dict_row['Sleep.duration.hrs'] += duration_hrs

            # Time to fall asleep
            if i == 0:  # first/main sleep segment (non nap)
                dict_row['Sleep.timeTookToFallAsleep.hrs'] = est_post_report_sleep_delay

        # Calculate wake events
        if len(sleep_segments) > 1:
            for seg in sleep_segments:
                wake_time = seg[-1][1]
                if wake_time.hour < ASSUMPTIONS['wakeEventsTimeFromWhenLikelyOnlyAlarm'].hour:
                    dict_row['Sleep.interruptions.natural'] += 1
                else:
                    dict_row['Sleep.interruptions.alarm'] += 1
                dict_row['Sleep.interruptions.tot'] += 1

        df.loc[waking_date, list(dict_row.keys())] = list(dict_row.values())
    t1 = datetime.now()
    print('Sleep data: Parsing sleep segments: ended in x seconds: ', (t1 - t0).seconds)

    # Attempt 1
    # Combine retro with non retro events into single time series
    # for _index, row in input_df.iterrows():
    #     row = dict(row)
    #     event = row[FLD['event']]
    #     event_past = row[FLD['event_past']]
    #     start_stop = row[FLD['start_stop']]
    #     start_stop_past = row[FLD['start_stop_past']]
    #     timestamp = row['Timestamp']
    #     timestamp_past = row[FLD['timestamp_past']]
    #     this_date: date = timestamp.date()
    #
    #     # Determine target field & timestamp
    #     if event == EVENT_INDICATOR_MAP['Sleep']:
    #         target_field = 'Sleep.start' if start_stop == 'Start' else 'Sleep.wake'
    #         target_timestamp = timestamp
    #     elif event_past == EVENT_INDICATOR_MAP['Sleep']:
    #         target_field = 'Sleep.start' if start_stop_past == 'Start' else 'Sleep.wake'
    #         target_timestamp = timestamp_past
    #     else:
    #         continue  # Row has no related activity
    #
    #     # Set values
    #     df.at[this_date, target_field] = target_timestamp
    #
    # # Add sleep start/stop to their appropriate 'waking days'
    # df2 = copy(df)
    # # todo: filter out NaT beforehand if can, rather than in loop?
    # # https://stackoverflow.com/questions/54738858/how-to-fill-nat-and-nan-values-separately
    # # u = df2.select_dtypes(include=['datetime'])
    # # df2[u.columns] = u.fillna('')
    # # todo: try/except: should not need. if no events on date, an empty row should bed in input_df
    # for _index, row in df.iterrows():
    #     row = dict(row)
    #     sleep_start: Timestamp = row['Sleep.start']
    #     sleep_end: Timestamp = row['Sleep.wake']
    #     prev_day: date = row['Date'] - timedelta(days=1)
    #     if sleep_start:
    #         sleep_after_midnight = sleep_start.hour < 12
    #         # noinspection PyTypeChecker
    #         sleep_start_day = prev_day if sleep_after_midnight else row['Date']
    #         # sleep_start_hour_minutes = str(sleep_start).split()[1]
    #         sleep_start_hour_minutes = sleep_start.time()
    #         try:
    #             df2.at[sleep_start_day, 'Sleep.startFromWakingDayStartedTime'] = sleep_start_hour_minutes
    #             df2.at[sleep_start_day, 'Sleep.startFromWakingDayStartedDatetime'] = sleep_start
    #         except IndexError:  # no row for that previous date
    #             print('Could not find row with date: ', sleep_start_day, file=sys.stderr)
    #
    #     if sleep_end:
    #         # sleep_end_hour_minutes = str(sleep_end).split()[1]
    #         sleep_end_hour_minutes = sleep_end.time()
    #         try:
    #             df2.at[prev_day, 'Sleep.endFromWakingDayStartedTime'] = sleep_end_hour_minutes
    #             df2.at[prev_day, 'Sleep.endFromWakingDayStartedDatetime'] = sleep_end
    #         except IndexError:  # no row for that previous date
    #             pass  # already printed err probably
    #
    # df2 = df2.fillna('')
    # # todo: i should fix it so this doesn't happen and i don't need to do this subset. I think this happens when
    # #  I run after cache date, or somehow imputing in future. not sure
    # df2 = df2[df2['Date'] != '']
    #
    # # Add sleep duration
    # # todo: FutureWarning: Inferring timedelta64[ns] from data containing strings is deprecated and will be
    # #  removed in a future version. To retain the old behavior explicitly pass Series(data, dtype={value.dtype})
    # #  - This can be fixed if I just keep NaT's instead of '', but would then need to look for them instead
    # df2['Sleep.durationFromWakingDayStarted'] = df2.apply(
    #     lambda x: round(
    #         x['Sleep.endFromWakingDayStartedDatetime'] - x['Sleep.startFromWakingDayStartedDatetime'] / (60 * 60), 2)
    #     if x['Sleep.endFromWakingDayStartedDatetime'] != '' and x['Sleep.startFromWakingDayStartedDatetime'] != ''
    #     else '', axis=1)

    return df


# TODO: not performant. I think it's these lines below. look at how i did sleep data; mighta done differently
#         target_index = df2.index[df2['Date'] == date_str].tolist()[0]
#         df2.at[target_index, target_field] = target_timestamp
def transform_and_impute(
    input_df: DataFrame, exclude_gaming_data=False, exclude_alcohol_data=False, exclude_sleep_data=False
) -> DataFrame:
    """Add missing dates, add weekdays, set index as dates, & also imputes a lot of speicifc reporting data"""
    # Setup
    column_names = [
        'Date',
        'Weekday']
    if not exclude_gaming_data:
        column_names += [
            'GamesFriends.start', 'GamesFriends.stop', 'GamesFriends.pct', 'GamesFriends.tot', 'GamesSolo.start',
            'GamesSolo.stop', 'GamesSolo.pct', 'GamesSolo.tot', 'Games.tot']
    if not exclude_alcohol_data:
        column_names += [
            'Drinks.tot']
    if not exclude_sleep_data:
        column_names += [
            # 'Sleep.start',
            # 'Sleep.wake',
            # 'Sleep.startFromWakingDayStartedTime',
            # 'Sleep.endFromWakingDayStartedTime',
            # 'Sleep.startFromWakingDayStartedDatetime',
            # 'Sleep.endFromWakingDayStartedDatetime',
            # 'Sleep.durationHrsFromWakingDayStarted',
            'Sleep.start.timestamp',
            'Sleep.end.mainSegement.timestamp',
            'Sleep.end.allSegments.timestamp',
            'Sleep.duration.hrs',
            'Sleep.timeTookToFallAsleep.hrs',
            'Sleep.interruptions.natural',
            'Sleep.interruptions.alarm',
            'Sleep.interruptions.tot',
        ]
    column_names += ['Comments.all']  # TODO: populate this column too
    df = DataFrame(columns=column_names)

    # Set columms: Date, Weekday & fill any missing rows for any missing dates
    # todo: Data should come in sorted but might want to add sorting just in case
    #  - I did find an exception where it's not sorted. Sometimes I add a manual row directly to the google sheet
    #  instead of using the form, and under some circumstances (perhaps if I added it to the bottom?), new rows added
    #  by the form do not get inserted below it, but all get inserted above it, leading to 1 out of order row at end
    first_date, last_date = [x.date() for x in input_df.iloc[[0, -1]]['Timestamp'].to_dict().values()]
    df['Date'] = [first_date + timedelta(days=x) for x in range((last_date - first_date).days + 1)]
    df['Weekday'] = df.get('Date').apply(_weekday_from_date)
    df = df.fillna('')
    df['Date2'] = df['Date']
    df = df.set_index('Date2')

    if not exclude_gaming_data:
        df = transform_gaming(input_df, df)
    if not exclude_alcohol_data:
        df = transform_alcohol(input_df, df)
    if not exclude_sleep_data:
        df = transform_sleep(input_df, df)

    return df
