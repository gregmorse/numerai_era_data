from datetime import date

from numerai_era_data.data_sources.base_data_source import BaseDataSource
from numerai_era_data.data_sources.ds_wei import DataSourceWEI


def test_get_data():
    ds_wei = DataSourceWEI()
    ds_data = ds_wei.get_data(date(2012, 1, 1), 
                              date(2022, 1, 1))
    
    assert ds_data.iloc[0][BaseDataSource.DATE_COL] == date(2012, 1, 1)
    assert ds_data.iloc[-1][BaseDataSource.DATE_COL] == date(2022, 1, 1)


def test_get_data_single_date():
    ds_wei = DataSourceWEI()
    ds_data = ds_wei.get_data(date(2012, 1, 1), 
                              date(2012, 1, 1))
    
    assert ds_data.shape[0] == 1
    assert ds_data.iloc[0][BaseDataSource.DATE_COL] == date(2012, 1, 1)
    assert ds_data[DataSourceWEI.COLUMN_WEI].iloc[0] > 0


def test_get_data_cutoff():
    ds_wei = DataSourceWEI()
    ds_data = ds_wei.get_data(date(2023, 6, 1), 
                              date(2023, 6, 2))
    
    assert ds_data[DataSourceWEI.COLUMN_WEI].iloc[0] != ds_data[DataSourceWEI.COLUMN_WEI].iloc[1]


def test_get_columns():
    ds_wei = DataSourceWEI()
    ds_columns = ds_wei.get_columns()
    ds_data = ds_wei.get_data(date(2012, 1, 1),
                              date(2012, 1, 8))
    
    data_columns = [column for column in ds_data.columns if column != BaseDataSource.DATE_COL]
    assert ds_columns == data_columns
    
test_get_data()