"""Transform data into a table showing gaming time spent by modality (friends
vs solo), by date and weekday.
"""
import datetime
from dateutil.parser import parse

from numpy import ndarray
from pandas import DataFrame, Series


ASSUMPTIONS = {
    'gamingEarliestDailyStart': '9:30:00',  # hh:MM:SS
}


def _weekday_from_date(val: str) -> str:
    """Get weekday from date"""
    week_days = [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
        "Sunday"]
    month, day, year = val.split('/')
    week_num = datetime.date(int(year), int(month), int(day)).weekday()
    weekday = week_days[week_num]
    return weekday


def transform(df: DataFrame) -> DataFrame:
    """Transform data
    TODO: calc each of these fields, 1 at a time
    """
    column_names = [
        'Date', 'Weekday', 'GamesFriends.start', 'GamesFriends.stop',
        'GamesFriends.pct', 'GamesFriends.tot', 'GamesSolo.start',
        'GamesSolo.stop', 'GamesSolo.pct', 'GamesSolo.tot', 'Games.tot']
    df2 = DataFrame(columns=column_names)
    timestamps: Series = df.get('Timestamp')
    dates: ndarray = timestamps.apply(lambda x: x.split()[0]).unique()
    df2['Date'] = dates.tolist()
    df2['Weekday'] = df2.get('Date').apply(_weekday_from_date)

    # TODO: format timeseires cols. in google sheets *
    # ...datetime()

    # TODO: Get GameFriends.start and GamesSolo.start
    # ...can do this simply right now, using no inference or modeling based on
    # ...known factors and educated guesses about my behavior when inputting
    # data. but for now can treat both same and be naiive and just l ook for
    # start time if it there and if not infer from end.

    for activity_start_field in ['GamesFriends.start', 'GamesSolo.start']:
        for date in dates:  # str
            # https://stackoverflow.com/questions/51589573/pandas-filter-data-
            # frame-rows-by-function
            date = parse(date)
            earliest_time_expected = parse(str(date) + ' ' + \
                 str(ASSUMPTIONS['gamingEarliestDailyStart']))
            earliest_time_expected_next_day = date + datetime.timedelta(days=1)

            # TODO: filter all relevant timestamps
            # TODO: *format cols first
            events_of_day_stamps = (df['Timestamp'] >= earliest_time_expected) \
                & (df['Timestamp'] < earliest_time_expected_next_day)
            print()

    return df2
