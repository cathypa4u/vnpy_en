from datetime import datetime, timedelta
import exchange_calendars


ANNUAL_DAYS = 240

# Get public holidays data from Shanghai Stock Exchange
cn_calendar: exchange_calendars.ExchangeCalendar = exchange_calendars.get_calendar('XSHG')
holidays: list = [x.to_pydatetime() for x in cn_calendar.precomputed_holidays()]

# Filter future public holidays
start: datetime = datetime.today()
PUBLIC_HOLIDAYS = [x for x in holidays if x >= start]


def calculate_days_to_expiry(option_expiry: datetime) -> int:
    """"""
    current_dt: datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days: int = 1
    
    # [추가된 코드] option_expiry가 timezone 정보를 가지고 있다면, current_dt도 동일하게 맞춤
    if option_expiry.tzinfo is not None:
        current_dt = current_dt.replace(tzinfo=option_expiry.tzinfo)
    # [추가된 코드] 반대로 option_expiry가 없고 current_dt만 있는 경우(드물지만)를 대비해 제거
    elif current_dt.tzinfo is not None:
        current_dt = current_dt.replace(tzinfo=None)
    # [수정 코드 끝]
    
    while current_dt < option_expiry:
        current_dt += timedelta(days=1)

        # Ignore weekends
        if current_dt.weekday() in [5, 6]:
            continue

        # Ignore public holidays
        if current_dt in PUBLIC_HOLIDAYS:
            continue

        days += 1

    return days
