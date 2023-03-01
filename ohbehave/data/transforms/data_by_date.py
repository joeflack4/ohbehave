"""Transform GoogleForms submissiond ata into a 1row=1day DataFrame
"""
import os
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
    """Get sleep summary stats from a data_by_date DF

    Uploaded to: https://drive.google.com/drive/folders/1vlFKH_9XqGDLpO6afyu_vvHrgQ41PSZV

    Codebook
    Sleep.start.hr:	This is the time that I started sleeping for the given day. E.g. if it's Tuesday and says 3am, it
     means I started sleeping at 3am Wednesday morning.
    Sleep.end.mainSegment.hr:	Applies to the "main segment" of sleep. That is, if I had any naps the next day after
     sleeping, this is when I woke up before any of those naps. This is the time that I stopped sleeping from the start
     of sleep from a given waking day. E.g. if it's Tuesday and says 1pm, it means I stopped sleeping Wednesday at 1pm.
    Sleep.end.allSegments.hr:	This is the end time of any sleep events I had from the starting waking day (main
     segment, plus any naps).  E.g. if it's Tuesday and says 3pm, it means I woke up some time at Wednesday, and may /
     may not have taken 1 or more naps, the last of which ended at 3pm.
    Sleep.duration.hrs:	Total duration of sleep, main segment, plus any naps, starting from the original waking day,
     and ending on the next day.
    Sleep.interruptions.natural:	Any times that I logged that I woke up "during the night"; that is, before any alarm
     or someone else waking me up at an expected time.
    Sleep.interruptions.alarm:	Any excess times I logged that I woke up due to an alarm clock or other expected
     interruption (e.g. someone waking me up). The last alarm is not considedred an interruption, but planned. So if I
     woke up due to an alarm, then took a nap, then woke up again, the value for this would be 1.
     Sleep.interruptions.tot:	The total of Sleep.interruptions.natural + Sleep.interruptions.alarm

    todo: more stats to consider:
    # 6	median()	Median of Values
    # 9	mode()	Mode of Values  # val that occurs most often"""
    stat_columns = [
        'Sleep.start.hr',
        'Sleep.end.mainSegment.hr',
        'Sleep.end.allSegments.hr',
        'Sleep.duration.hrs',
        'Sleep.interruptions.natural',
        'Sleep.interruptions.alarm',
        'Sleep.interruptions.tot',
    ]
    weekday_map = {
        'All': 0,
        'Sunday': 1,
        'Monday': 2,
        'Tuesday': 3,
        'Wednesday': 4,
        'Thursday': 5,
        'Friday': 6,
        'Saturday': 7,
    }
    am_pm_cols = [
        'Sleep.start.hr',
        'Sleep.end.mainSegment.hr',
        'Sleep.end.allSegments.hr',
    ]

    # - Filter out rows with null durations / etc
    input_df = input_df[input_df['Sleep.duration.hrs'] != '']

    # Filter outliers# (see workflowy)
    # - Sleep.duration.hrs > 16. This is where I'm drawing an arbitrary line. In reality, one of them said 32 total
    #   ...hours with like 5 sleep interruptions.
    input_df = input_df[input_df['Sleep.duration.hrs'] < 16]

    # Generate stats
    rows = []
    for weekday, df_i in input_df.groupby(by='Weekday'):
        for col in stat_columns:
            val = round(df_i[col].mean(), 2)
            if col in am_pm_cols:
                val = time(hour=int(val), minute=int((val % 1) * 60)).strftime('%H:%M %p')
            rows.append({'Weekday': weekday, 'Metric': col, 'Agg.': 'Avg', 'Value': val})
            val = round(df_i[col].std(), 2)
            rows.append({'Weekday': weekday, 'Metric': col, 'Agg.': 'StdDev', 'Value': val})
    for col in stat_columns:
        val = round(input_df[col].mean(), 2)
        if col in am_pm_cols:
            val = time(hour=int(val), minute=int((val % 1) * 60)).strftime('%H:%M %p')
        rows.append({'Weekday': 'All', 'Metric': col, 'Agg.': 'Avg', 'Value': val})
        val = round(input_df[col].std(), 2)
        rows.append({'Weekday': 'All', 'Metric': col, 'Agg.': 'StdDev', 'Value': val})

    # Formatting
    # - Weekday numbers for sorting (kinda hacky here maybe? but w/e)
    df = pd.DataFrame(rows)
    df['weekday_num'] = df['Weekday'].apply(lambda x: weekday_map[x])
    df = df.sort_values(['Agg.', 'weekday_num']).drop(columns=['weekday_num'])

    return df


# TODO: not performant
# TODO: datetime64 column dtype
def data_by_date(
    exclude_gaming_data=False, exclude_alcohol_data=False, exclude_sleep_data=False, ignore_gsheets_cache=False,
    verbose=False, use_cache=False, df_cache_path=os.path.join(CACHE_DIR, 'data_by_date.pickle')
) -> DataFrame:
    """Get data by date, 1row=1date"""
    if use_cache:
        file = open(df_cache_path, 'rb')
        df = pickle.load(file)
        file.close()
    else:
        df: DataFrame = get_sheets_data(datetime.now() - timedelta(days=7), ignore_gsheets_cache)
        df = transform_and_impute(df, exclude_gaming_data, exclude_alcohol_data, exclude_sleep_data, verbose)
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


def transform_sleep(input_df: pd.DataFrame, df: pd.DataFrame, verbose=False) -> pd.DataFrame:
    """Transform gaming data"""
    # todo: if this is more performant, reimplement everything to use this method
    # d = {x['Date']: x for x in df.to_dict(orient='records')}

    # Attempt 2
    # temp comment: fundamentally different than attempt 1. Will probably be different num/nature of iteration
    # Step 1: Collect all sleep events for a each sleep start day
    date_events_map: Dict[date, List[EVENT_TYPE]] = {}
    for _index, row in input_df.iterrows():
        row = dict(row)
        # todo: duplicated code fragment here. can probably generalize between these transform_* funcs
        # noinspection DuplicatedCode
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
    # todo: If I don't have a sleep start OR a sleep end, find some way to ignore for the day for now
    #  - am I already catching this case?
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

        # todo: in future I want to explicitly impute. See Workflowy.
        if not sleep_segments:
            continue
        elif sleep_segments[0][0][0] == 'Sleep.wake':  # Many cases, actually
            if verbose:
                print('Missing sleep start on: ', waking_date)
            continue

        dict_row['Sleep.interruptions.natural'] = 0
        dict_row['Sleep.interruptions.alarm'] = 0
        dict_row['Sleep.interruptions.tot'] = 0
        dict_row['Sleep.start.timestamp'] = sleep_segments[0][0][1]
        dict_row['Sleep.end.mainSegment.timestamp'] = sleep_segments[0][-1][1]
        dict_row['Sleep.end.allSegments.timestamp'] = sleep_segments[-1][-1][1]
        dict_row['Sleep.start.hr'] = sleep_segments[0][0][1].hour
        dict_row['Sleep.end.mainSegment.hr'] = sleep_segments[0][-1][1].hour
        dict_row['Sleep.end.allSegments.hr'] = sleep_segments[-1][-1][1].hour
        dict_row['Sleep.duration.hrs'] = 0

        # Calc duration & time taken to fall asleep (non naps)
        for i, seg in enumerate(sleep_segments):
            # Sleep duration
            if i == 0:  # first/main sleep segment. typically takes a lot longer to fall sleep
                est_post_report_sleep_delay = ASSUMPTIONS['avgTimeAfterLoggingToFirstSleepIfLogged1x'] \
                    if len(seg) <= 2 else ASSUMPTIONS['avgTimeAfterLoggingToFirstSleepIfLogged2+']
            else:  # nap. typically takes less time to fall asleep
                est_post_report_sleep_delay = ASSUMPTIONS['avgTimeAfterLoggingToFirstSleepIfLogged2+']
            try:
                last_reported_sleep_start = seg[-2][1]
            except IndexError:  # See: secondarySleepSegmentErr# in Workflowy
                if verbose:
                    print('Secondary sleep segment missing Sleep.start. Skipping: ', seg)
                continue
            est_actual_sleep_start = last_reported_sleep_start + est_post_report_sleep_delay
            sleep_end = seg[-1][1]
            duration_hrs = _timedelta_to_hrs(sleep_end - est_actual_sleep_start)
            # if duration_hrs < 0:  # haven't seen this happen
            #     print(f'Unexpected negative sleep duration: {duration_hrs}: ', seg)
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

    return df


# TODO: not performant. I think it's these lines below. look at how i did sleep data; mighta done differently
#         target_index = df2.index[df2['Date'] == date_str].tolist()[0]
#         df2.at[target_index, target_field] = target_timestamp
def transform_and_impute(
    input_df: DataFrame, exclude_gaming_data=False, exclude_alcohol_data=False, exclude_sleep_data=False, verbose=False
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
            'Sleep.end.mainSegment.timestamp',
            'Sleep.end.allSegments.timestamp',
            'Sleep.start.hr',
            'Sleep.end.mainSegment.hr',
            'Sleep.end.allSegments.hr',
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
        df = transform_sleep(input_df, df, verbose)

    return df
