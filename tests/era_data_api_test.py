import os
from datetime import date, timedelta

import pandas as pd
import pytest
from mock import MagicMock, patch

from numerai_era_data import era_data_api
from numerai_era_data.data_sources.base_data_source import BaseDataSource
from numerai_era_data.date_utils import ERA_ONE_START, get_date_for_era


class MockDataSource:
    def __init__(self):
        pass

    def get_columns(self):
        return ["column1"]

    def get_data(self, start_date, end_date):
        df = pd.DataFrame()
        if start_date == get_date_for_era(1):
            df = pd.concat([df, pd.DataFrame({BaseDataSource.DATE_COL: [ERA_ONE_START], "column1": [1]})])
        if end_date >= get_date_for_era(2):
            df = pd.concat([df, pd.DataFrame(
                {BaseDataSource.DATE_COL: [ERA_ONE_START + timedelta(days=7)], "column2": [2]})])
        if end_date >= get_date_for_era(3):
            df = pd.concat([df, pd.DataFrame(
                {BaseDataSource.DATE_COL: [ERA_ONE_START + timedelta(days=14)], "column3": [3]})])
        if end_date >= get_date_for_era(4):
            df = pd.concat([df, pd.DataFrame(
                {BaseDataSource.DATE_COL: [ERA_ONE_START + timedelta(days=21)], "column4": [4]})])
        if start_date == date(2001, 4, 20):
            df = pd.DataFrame({BaseDataSource.DATE_COL: [date(2001, 4, 20)], "column5": [5]})

        return df


class MockDataSourceWithException:
    def __init__(self):
        pass

    def get_columns(self):
        return ["column2", "column3"]

    def get_data(self, start_date, end_date):
        raise Exception("Test exception")


@pytest.fixture
def manage_cache():
    instance = era_data_api.EraDataAPI()
    instance.DATA_CACHE_FILE = "test_data_cache.parquet"
    instance.DAILY_CACHE_FILE = "test_daily_cache.parquet" 

    yield instance

    if os.path.exists(instance.DATA_CACHE_FILE):
        os.remove(instance.DATA_CACHE_FILE)
    if os.path.exists(instance.DAILY_CACHE_FILE):  
        os.remove(instance.DAILY_CACHE_FILE)


def test_get_data_sources():
    data_api = era_data_api.EraDataAPI()
    data_sources = data_api._get_data_sources()

    assert len(data_sources) > 0
    for data_source_class in data_sources:
        assert issubclass(data_source_class, era_data_api.BaseDataSource)
        assert data_source_class != BaseDataSource


def test_get_data_sources_returns_cached_list():
    instance = era_data_api.EraDataAPI()
    instance.class_cache = ['cached_data']

    result = instance._get_data_sources()

    assert result == ['cached_data']


def test_update_data_with_empty_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame()
    
    with patch("numerai_era_data.date_utils.get_current_era", return_value=3):
        instance.update_data()

    assert not instance.data_cache.empty
    assert instance.data_cache["era"].tolist() == ["0001", "0002", "0003"]
    assert instance.data_cache.columns.tolist() == ["era", "column1", "column2", "column3"]


def test_update_data_with_existing_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=4):
        instance.update_data()

    assert not instance.data_cache.empty
    assert instance.data_cache["era"].tolist() == ["0001", "0002", "0003", "0004"]
    assert instance.data_cache.columns.tolist() == ["era", "column1", "column2", "column3", "column4"]


def test_update_data_with_exception(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSourceWithException])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=3):
        instance.update_data()

    assert not instance.data_cache.empty
    assert instance.data_cache["era"].tolist() == ["0001", "0002", "0003"]
    assert instance.data_cache.columns.tolist() == ["era", "column1", "column2", "column3"]

def test_update_daily_data_with_empty_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.daily_cache = pd.DataFrame()
    data_date = date(2001, 4, 20)
    
    with patch("numerai_era_data.date_utils.get_current_date", return_value=data_date):
        instance.update_daily_data()

    assert not instance.daily_cache.empty
    assert instance.daily_cache[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert instance.daily_cache.columns.tolist() == [BaseDataSource.DATE_COL, "column5"]


def test_update_daily_data_with_existing_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.daily_cache = pd.DataFrame({BaseDataSource.DATE_COL: ["1234"], "column1": [1]})
    data_date = date(2001, 4, 20)

    with patch("numerai_era_data.date_utils.get_current_date", return_value=data_date):
        instance.update_daily_data()

    assert not instance.daily_cache.empty
    assert instance.daily_cache[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert instance.daily_cache.columns.tolist() == [BaseDataSource.DATE_COL, "column5"]


def test_update_daily_data_with_exception(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSourceWithException])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column2": [2], "column3": [3]})
    instance.daily_cache = pd.DataFrame({BaseDataSource.DATE_COL: ["1234"], "column1": [1]})
    data_date = date(2001, 4, 20)

    with patch("numerai_era_data.date_utils.get_current_date", return_value=data_date):
        instance.update_daily_data()

    assert not instance.daily_cache.empty
    assert instance.daily_cache[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert instance.daily_cache.columns.tolist() == [BaseDataSource.DATE_COL, "column2", "column3"]


def test_get_current_era_no_update(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=1):
        df = instance.get_current_era()
    
    assert df["era"].tolist() == ["0001"]


def test_get_current_era_with_update(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=2):
        df = instance.get_current_era()
    
    assert df["era"].tolist() == ["0002"]


def test_get_current_era_with_update_and_no_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame()

    with patch("numerai_era_data.date_utils.get_current_era", return_value=1):
        df = instance.get_current_era()
    
    assert df["era"].tolist() == ["0001"]


def test_get_current_era_columns_changed(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column0": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=1):
        df = instance.get_current_era()
    
    assert df["era"].tolist() == ["0001"]
    assert df.columns.tolist() == ["era", "column0", "column1"]


def test_get_current_era_no_update_stale(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=3):
        df = instance.get_current_era(False)
    
    assert df["era"].tolist() == ["0001"]


def test_get_all_eras_no_update(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=1):
        df = instance.get_all_eras()
    
    assert df["era"].tolist() == ["0001"]


def test_get_all_eras_with_update(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=2):
        df = instance.get_all_eras()
    
    assert df["era"].tolist() == ["0001", "0002"]


def test_get_all_eras_with_update_and_no_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame()

    with patch("numerai_era_data.date_utils.get_current_era", return_value=2):
        df = instance.get_all_eras()
    
    assert df["era"].tolist() == ["0001", "0002"]


def test_get_all_eras_columns_changed(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column0": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=1):
        df = instance.get_all_eras()
    
    assert df.columns.tolist() == ["era", "column0", "column1"]


def test_get_all_eras_no_update_stale(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    instance.data_cache = pd.DataFrame({"era": ["0001"], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_era", return_value=3):
        df = instance.get_all_eras(False)
    
    assert df["era"].tolist() == ["0001"]


def test_get_current_daily_no_update(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    data_date = date(2022, 1, 1)
    instance.daily_cache = pd.DataFrame({BaseDataSource.DATE_COL: [data_date], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_date", return_value=data_date):
        df = instance.get_current_daily()
    
    assert df[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert df["column1"].tolist() == [1]


def test_get_current_daily_with_update(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    cache_data_date = date(2022, 1, 1)
    data_date = date(2001, 4, 20)
    instance.daily_cache = pd.DataFrame({BaseDataSource.DATE_COL: [cache_data_date], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_date", return_value=data_date):
        df = instance.get_current_daily()

    assert df[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert df["column5"].tolist() == [5]


def test_get_current_daily_with_update_and_no_cache(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    data_date = date(2001, 4, 20)
    instance.daily_cache = pd.DataFrame()

    with patch("numerai_era_data.date_utils.get_current_date", return_value=data_date):
        df = instance.get_current_daily()

    assert df[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert df["column5"].tolist() == [5]


def test_get_current_daily_no_update_stale(manage_cache):
    instance = manage_cache
    instance._get_data_sources = MagicMock(return_value=[MockDataSource])
    data_date = date(2022, 1, 1)
    request_date = date(2022, 5, 1)
    instance.daily_cache = pd.DataFrame({BaseDataSource.DATE_COL: [data_date], "column1": [1]})

    with patch("numerai_era_data.date_utils.get_current_date", return_value=request_date):
        df = instance.get_current_daily(False)
    
    assert df[BaseDataSource.DATE_COL].tolist() == [data_date]
    assert df["column1"].tolist() == [1]
