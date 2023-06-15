from datetime import date, datetime, timezone

from mock import patch

from numerai_era_data.date_utils import (get_current_date, get_current_era,
                                         get_date_for_era, get_era_for_date)


def test_get_current_era():
    assert get_current_era() > 0

def test_get_current_date_before_noon():
    with patch("numerai_era_data.date_utils.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2023, 5, 27, 11, 59, 0, 0, timezone.utc)
        assert get_current_date() == date(2023, 5, 26)

def test_get_current_date_after_noon():
    with patch("numerai_era_data.date_utils.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2023, 5, 27, 12, 0, 0, 0, timezone.utc)
        assert get_current_date() == date(2023, 5, 27)

def test_get_era_for_date_identity():
    assert get_era_for_date(get_date_for_era(1)) == 1

def test_get_era_for_date_day_before():
    assert get_era_for_date(date(2023, 5, 26)) == 1063

def test_get_era_for_date_day_after():
    assert get_era_for_date(date(2023, 5, 27)) == 1064

def test_get_date_for_era_1():
    assert get_date_for_era(1) == date(2003, 1, 11)

def test_get_date_for_era_1063():
    assert get_date_for_era(1063) == date(2023, 5, 20)
