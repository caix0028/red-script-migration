from datetime import date, datetime
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta


def get_querytimes(fs_date_start: date, fs_date_end: date):
    month_diff = (
        fs_date_end.month
        - fs_date_start.month
        + 12 * (fs_date_end.year - fs_date_start.year)
    )

    if month_diff == 0:
        querytimes = [
            int(
                datetime.combine(fs_date_start, datetime.min.time())
                .replace(tzinfo=ZoneInfo("UTC"))
                .timestamp()
                * 1000
            )
        ]
    else:
        begin_time = datetime.combine(fs_date_start.replace(day=1), datetime.min.time())
        querytimes = [begin_time.replace(tzinfo=ZoneInfo("UTC")).timestamp()]
        for i in range(1, month_diff + 1):
            querytimes.append(
                (begin_time + relativedelta(months=i))
                .replace(tzinfo=ZoneInfo("UTC"))
                .timestamp()
            )
        querytimes = [int(i * 1000) for i in querytimes]
    return querytimes
