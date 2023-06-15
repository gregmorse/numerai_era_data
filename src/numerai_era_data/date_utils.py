from datetime import date, datetime, timedelta, timezone

ERA_ONE_START = date(2003, 1, 11)


def get_current_era() -> int:
    return get_era_for_date(get_current_date())


def get_current_date() -> date:
    # noon UTC is the cutoff for the current date, will return yesterday's date if before noon
    return (
        datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        - timedelta(days=datetime.now().hour < 12)
    ).date()


def get_era_for_date(date: date) -> int:
    return (date - ERA_ONE_START).days // 7 + 1  # era is 1-indexed


def get_date_for_era(era: int) -> date:
    return ERA_ONE_START + timedelta(days=(era - 1) * 7)
