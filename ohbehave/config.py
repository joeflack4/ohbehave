"""Config"""
import os.path
from datetime import time, timedelta

APP_ROOT = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.join(APP_ROOT, '..')
ENV_DIR = os.path.join(PROJECT_ROOT, 'env')
CACHE_DIR = os.path.join(APP_ROOT, 'data', 'cache')

ASSUMPTIONS = {
    # General
    'maxHoursRetroWasUsedToActuallyReportFutureEvent': 2,  # dk if i'm using this
    # Gaming
    'gamingEarliestDailyStart': '9:30:00',  # hh:MM:SS
    # Sleep
    'avgTimeAfterLoggingToFirstSleepIfLogged1x': timedelta(hours=1.25),
    'avgTimeAfterLoggingToFirstSleepIfLogged2+': timedelta(minutes=20),
    'latestExpectedSleepHour': time(hour=7),  # 7am
    'wakeEventsTimeFromWhenLikelyOnlyAlarm': time(hour=7),  # 7am  todo: >~1pm on weekends
    'sleepStartTimeFromWhenNonNap': time(hour=20),  # 8pm
}
