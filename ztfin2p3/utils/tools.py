
def parse_singledate(date):
    """ parse the input 'date' into a datetime start and end
    date could have the following format:
        - yyyymm: get the full month 
                 (stars at day 1, ends day 1 next month)
        - yyyywww: get the full calendar week
                 (stars the monday of day 1, ends the next monday)
        - yyyymmdd: get the full day
                 (stars the day, ends the next)
    """
    import datetime
    date = str(date)
    if len(date) == 6: # year+month
        from calendar import monthrange
        year, month = int(date[:4]),int(date[4:])
        date_start = datetime.date.fromisoformat(f"{year}-{month:02d}-01")
        # Doing this avoid to think about end of year issues
        date_end   = date_start + datetime.timedelta(days=monthrange(year, month)[1]) # +1 month = N(28-31) days

    elif len(date) == 7: # year+week
        year, week = int(date[:4]),int(date[4:])
        date_start = datetime.date.fromisocalendar(year, week, 1)
        date_end   = date_start + datetime.timedelta(weeks=1) # +1 week

    elif len(date) == 8: # year+month+day
        year, month, day = int(date[:4]),int(date[4:6]),int(date[6:])
        date_start = datetime.date.fromisoformat(f"{year:04d}-{month:02d}-{day:02d}")
        date_end   = date_start + datetime.timedelta(days=1)
    else:
        raise ValueError(f"Cannot parse the input single date format {date}, size 6(yyyymm), 7(yyyywww) or 8(yyyymmdd) expected")
        
    return date_start, date_end
