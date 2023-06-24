from datetime import date, datetime

import pytz

from numerai_era_data.data_sources.base_data_source import BaseDataSource
from numerai_era_data.data_sources.ds_bls import DataSourceBLS


def test_get_data():
    ds_bls = DataSourceBLS()
    ds_data = ds_bls.get_data(date(2012, 1, 1), date(2022, 1, 1))
    
    assert ds_data.iloc[0][BaseDataSource.DATE_COL] == date(2012, 1, 1)
    assert ds_data.iloc[-1][BaseDataSource.DATE_COL] == date(2022, 1, 1)
    assert round(ds_data.loc[ds_data[BaseDataSource.DATE_COL] == date(2012, 2, 17)] \
        [DataSourceBLS.COLUMN_CPI_U].values[0], 3) == 226.740
    assert round(ds_data.loc[ds_data[BaseDataSource.DATE_COL] == date(2012, 2, 18)] \
        [DataSourceBLS.COLUMN_CPI_U].values[0], 3) == 227.237
    

def test_get_data_today():
    ds_bls = DataSourceBLS()
    ds_data = ds_bls.get_data(date(2012, 1, 1), datetime.now(pytz.timezone('US/Eastern')).date())
        
    assert ds_data.iloc[-1][BaseDataSource.DATE_COL] == datetime.now(pytz.timezone('US/Eastern')).date()


def test_get_columns():
    ds_bls = DataSourceBLS()
    ds_columns = ds_bls.get_columns()
    ds_data = ds_bls.get_data(date(2012, 1, 1), date(2012, 1, 8))
    
    data_columns = [column for column in ds_data.columns if column != BaseDataSource.DATE_COL]
    assert ds_columns == data_columns
