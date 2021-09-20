"""Transform data into a table showing gaming time spent by modality (friends
vs solo), by date and weekday.
"""
import datetime

from numpy import ndarray
from pandas import DataFrame, Series


ASSUMPTIONS = {
    # gamingEarliestDailyStart: 9.5 = 9:30am
    'gamingEarliestDailyStart': 9,
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
    """Transform data"""
    column_names = [
        'Date', 'Weekday', 'GamesFriends.start', 'GamesFriends.stop',
        'GamesFriends.pct', 'GamesFriends.tot', 'GamesSolo.start',
        'GamesSolo.stop', 'GamesSolo.pct', 'GamesSolo.tot', 'Games.tot']
    df2 = DataFrame(columns=column_names)
    timestamps: Series = df.get('Timestamp')
    dates: ndarray = timestamps.apply(lambda x: x.split()[0]).unique()
    df2['Date'] = dates.tolist()
    df2['Weekday'] = df2.get('Date').apply(_weekday_from_date)

    return df2
