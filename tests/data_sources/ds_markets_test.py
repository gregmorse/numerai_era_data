from datetime import date, datetime

import pytz

from numerai_era_data.data_sources.base_data_source import BaseDataSource
from numerai_era_data.data_sources.ds_markets import DataSourceMarkets


def test_get_data():
    ds_markets = DataSourceMarkets()
    ds_data = ds_markets.get_data(date(2012, 1, 1), date(2022, 1, 1))
    
    assert ds_data.iloc[0][BaseDataSource.DATE_COL] == date(2012, 1, 1)
    assert ds_data.iloc[-1][BaseDataSource.DATE_COL] == date(2022, 1, 1)
    assert round(ds_data.loc[ds_data[BaseDataSource.DATE_COL] == date(2012, 10, 24)] \
        [DataSourceMarkets.COLUMN_SPX_CLOSE].values[0], 2) == 1413.11
    

def test_get_data_today():
    ds_markets = DataSourceMarkets()
    ds_data = ds_markets.get_data(date(2012, 1, 1), datetime.now(pytz.timezone('US/Eastern')).date())
        
    assert ds_data.iloc[-1][BaseDataSource.DATE_COL] == datetime.now(pytz.timezone('US/Eastern')).date()


def test_get_columns():
    ds_markets = DataSourceMarkets()
    ds_columns = ds_markets.get_columns()
    ds_data = ds_markets.get_data(date(2012, 1, 1), date(2012, 1, 8))
    
    data_columns = [column for column in ds_data.columns if column != BaseDataSource.DATE_COL]
    assert ds_columns == data_columns
