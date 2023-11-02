# return a time delta from now to input time

from datetime import datetime
import re, sys


def print_usage(prog):
    """This method prints a description of usage of the parsetime method of
    this script."""

    print('Time parser')
    print('Returns the time between now and the argument.')
    print('Argument time should be within one year (don\'t specify the year)')
    print(
        'Month and day can be written as \'Month Day\', \'MM/DD\', or \'m/DD\''
    )
    print('Colon separated time values are interpreted as:')
    print(
        '\'HH:MM\', \'HH:MM:SS\', \'DD:HH:MM:SS\' (leading zeroes not required)'
    )
    print('Hours may be specified in 12- or 24- hour format')
    print(
        'Use pm is required when using the 12-hour format to set a time, use of am is optional'
    )
    print('Enter a \'+\' symbol to specify the argument time is a duration')
    print('Separate different arguments with spaces')
    print('usage: ' + prog + ' time')
    print('examples:')
    print(prog + ' 3pm')
    print(prog + ' 1500 tomorrow')
    print(prog + ' 1500:30 today')
    print(prog + ' 3:00 pm Monday')
    print(prog + ' tues 3:00:30pm')
    print(prog + ' December 11th 03:00am')
    print(prog + ' 3pm 12/11')
    print(prog + ' 42:10:36:30')


def isleapyear(year):
    """Returns true if a given year is a leap year."""
    if year % 400 == 0 or (year % 4 == 0 and year % 100 != 0):
        return True
    return False


def numdays(current, future):
    """Returns a number of days in the future one weekday is from another."""
    # Wednesday to Tuesday
    if future < current:
        # 7 - Wednesday + Tuesday
        return 7 - current + future
    # Tuesday to Wednesday is Wednesday - Tuesday
    return future - current


def parsetime(timestr):
    """Returns an int( posix time ) from a provided input string.

    Interpreted strings can have relative offsets from the current time.

    This method will interpret many time strings and attempt to return
    the number of seconds until a future time.

    An invalid input will likely return a timedelta of about 24 hours in
    the future.
    """
    weekdays = {
        'mon': 0,
        'tue': 1,
        'wed': 2,
        'thu': 3,
        'fri': 4,
        'sat': 5,
        'sun': 6
    }
    months = {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr': 4,
        'may': 5,
        'jun': 6,
        'jul': 7,
        'aug': 8,
        'sep': 9,
        'oct': 10,
        'nov': 11,
        'dec': 12
    }
    # track whether the same day was given (it is monday and monday was given)
    # if the time would otherwise be in the past we can advance to next week
    now = datetime.now()
    # the future time, based off of now
    shouldbeinpast = False
    jumpweek = False
    ispm = False
    isduration = False
    second = now.second
    minute = now.minute
    hour = now.hour
    day = now.day
    month = now.month
    year = now.year
    plusdays = 0
    timestr = timestr.lower()
    if 'pm' in timestr:
        ispm = True
        timestr = re.sub('pm', ' ', timestr)
    elif 'am' in timestr:
        timestr = re.sub('am', ' ', timestr)
    timestr = re.sub(',', ' ', timestr)
    if timestr.find('+') >= 0:
        timestr = re.sub('+', '', timestr)
        isduration = True
    parts = timestr.split(' ')
    for part in parts:
        if part == '':
            pass
        elif part.isnumeric():
            if len(part) == 4:
                hour = int(part) // 100
                minute = int(part) % 100
            else:
                hour = int(part)
                minute = 0
            second = 0
        elif part == 'noon':  # 12pm
            hour = 12
            minute = 0
            second = 0
        elif 'yester' in part:  # yesterday
            plusdays = -1
            shouldbeinpast = True
        elif 'tom' in part:  # tomorrow
            plusdays = 1
        elif 'tod' in part:  # today
            pass
        elif ':' in part:
            pieces = part.split(':')
            # 42:10:3:12 <- specify to run for 42 days, 10 hours, ...,
            if len(pieces) == 4:
                plusdays = int(pieces[0])
                hour += int(pieces[1])
                minute += int(pieces[2])
                second += int(pieces[3])
            # 1720:30 <- 17 hours, 20 minutes, 30 seconds
            elif len(pieces) == 2 and len(pieces[0]) > 2:
                if isduration == True:
                    hour += int(pieces[0]) // 100
                    minute += int(pieces[0]) % 100
                    second += int(pieces[1])
                else:
                    hour = int(pieces[0]) // 100
                    minute = int(pieces[0]) % 100
                    second = int(pieces[1])
            # '+' symbol present, use additive duration
            elif isduration == True:
                hour += int(pieces[0])
                minute += int(pieces[1])
                if len(pieces) == 3:
                    second += int(pieces[2])
            # not a duration, set the time
            else:
                hour = int(pieces[0])
                minute = int(pieces[1])
                if len(pieces) == 3:
                    second = int(pieces[2])
                else:
                    second = 0
        elif '/' in part:
            pieces = part.split('/')
            month = pieces[0]
            day = pieces[1]
            jumpweek = False
        elif (part.endswith('st') or part.endswith('th') or part.endswith('nd')
              or part.endswith('rd')) and part[:-2].isnumeric():
            day = int(part[:-2])
        elif any(key in part for key in weekdays.keys()):
            for key in weekdays:
                if key in part:
                    plusdays = numdays(now.weekday(), weekdays[key])
                    break
            if plusdays == 0:
                jumpweek = True  # jump to next week if the time would otherwise be in the past
        elif any(key in part for key in months.keys()):
            for key in months:
                if key in part:
                    if months[key] < month:
                        year += 1
                    month = months[key]
                    break
            jumpweek = False
    if ispm == True:
        hour += 12
    while second > 59:
        second -= 60
        minute += 1
    while minute > 59:
        minute -= 60
        hour += 1
    while hour > 23:
        hour -= 24
        plusdays += 1
    day += plusdays
    # the time is in the past, bring it to the future
    # (if hour/min/sec not specified this can still end up being slightly in the past, e.g., ~1 minute in the past)
    if month <= now.month and year <= now.year:
        while day < now.day or (day == now.day and
                                (hour < now.hour or
                                 (hour == now.hour and minute < now.minute))):
            if shouldbeinpast:
                break
            # the day of the week was specified, move to next week
            if jumpweek == True:
                day += 7
            # no day was specified, move to tomorrow
            else:
                day += 1
    maxday = 31
    if month == 2:
        if isleapyear(month):
            maxday = 29
        else:
            maxday = 28
    elif month % 2 == 0:
        maxday = 30
    # we have day = 37, maxday = 31, do day-=31 ( == 6 ) and month +=1
    while day > maxday:
        day -= maxday
        month += 1
        if month > 12:
            month = 1
            year += 1
        maxday = 31
        if month == 2:
            if isleapyear(month):
                maxday = 29
            else:
                maxday = 28
        elif month % 2 == 0:
            maxday = 30
    while month > 12:
        month -= 12
        year += 1
    # datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None, *, fold=0)
    # timestamp to convert it to a POSIX timestamp
    # int cast to remove the trailing decimal
    return int(datetime(year, month, day, hour, minute, second).timestamp())


if __name__ == '__main__':
    """This script may be called as main to test output."""

    now = datetime.now()
    #print(now)
    if len(sys.argv) < 2:
        print_usage(sys.argv[0])
    else:
        print(parsetime(' '.join(sys.argv[1:])))
