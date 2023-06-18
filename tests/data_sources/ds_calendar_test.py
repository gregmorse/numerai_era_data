from datetime import date

from numerai_era_data.data_sources.base_data_source import BaseDataSource
from numerai_era_data.data_sources.ds_calendar import DataSourceCalendar


def test_get_data():
    ds_calendar = DataSourceCalendar()
    ds_data = ds_calendar.get_data(date(2012, 1, 1), 
                                   date(2022, 1, 1))
    
    assert ds_data.shape[0] == 3654
    assert ds_data.iloc[0][BaseDataSource.DATE_COL] == date(2012, 1, 1)
    assert ds_data.iloc[-1][BaseDataSource.DATE_COL] == date(2022, 1, 1)


def test_get_columns():
    ds_calendar = DataSourceCalendar()
    ds_columns = ds_calendar.get_columns()
    ds_data = ds_calendar.get_data(date(2012, 1, 1),
                                   date(2012, 1, 8))
    
    data_columns = [column for column in ds_data.columns if column != BaseDataSource.DATE_COL]
    assert ds_columns == data_columns
    