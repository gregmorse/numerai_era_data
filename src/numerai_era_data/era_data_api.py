import importlib
import inspect
import logging
import os
import pkgutil

import pandas as pd

import numerai_era_data.date_utils as date_utils
from numerai_era_data.data_sources.base_data_source import BaseDataSource


class EraDataAPI:
    CACHE_DIRECTORY = os.path.join(os.path.dirname(__file__), 'cache')
    DATA_CACHE_FILE = os.path.join(CACHE_DIRECTORY, 'data.parquet')
    DAILY_CACHE_FILE = os.path.join(CACHE_DIRECTORY, 'daily.parquet')

    def __init__(self):
        dir_name = os.path.dirname(self.DATA_CACHE_FILE)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        if os.path.exists(self.DATA_CACHE_FILE):
            self.data_cache = pd.read_parquet(self.DATA_CACHE_FILE)
        else:
            self.data_cache = pd.DataFrame()

        if os.path.exists(self.DAILY_CACHE_FILE):
            self.daily_cache = pd.read_parquet(self.DAILY_CACHE_FILE)
        else:
            self.daily_cache = pd.DataFrame()

        self.class_cache = []

        # logger config
        logging.basicConfig(filename="exception.log", level=logging.ERROR)

    def get_all_eras(self, update_if_stale=True) -> pd.DataFrame:
        update = False

        if update_if_stale:
            # if most current era is not in the data, update the data
            if self.data_cache.empty or self.data_cache[BaseDataSource.ERA_COL].astype(int).max() < date_utils.get_current_era():
                update = True

            # if any columns have been added since the last update, update the data
            for data_source_class in self._get_data_sources():
                data_source = data_source_class()
                if not set(data_source.get_columns()).issubset(set(self.data_cache.columns)):
                    update = True
                    break

        if update:
            self.update_data()

        return self.data_cache

    def get_current_daily(self, update_if_stale=True) -> pd.DataFrame:
        update = False

        if update_if_stale:
            # if most current era is not in the data, update the data
            if self.daily_cache.empty or self.daily_cache[BaseDataSource.DATE_COL][0] != date_utils.get_current_date():
                update = True

            # if any columns have been added since the last update, update the data
            for data_source_class in self._get_data_sources():
                data_source = data_source_class()
                if not set(data_source.get_columns()).issubset(set(self.daily_cache.columns)):
                    update = True
                    break

        if update:
            self.update_daily_data()

        return self.daily_cache

    def update_data(self):
        # update the cache
        new_data = pd.DataFrame()
        start_date = date_utils.get_date_for_era(1)
        end_date = date_utils.get_date_for_era(date_utils.get_current_era())
        
        for data_source_class in self._get_data_sources():
            data_source = data_source_class()

            try:
                data = data_source.get_data(start_date, end_date)
            except Exception as e:
                logging.exception(
                    f"Error getting data from {data_source_class.__name__}: {e} on {start_date} to {end_date}"
                )
                data = pd.DataFrame()
                data[BaseDataSource.DATE_COL] = pd.date_range(start_date, end_date)
                data[BaseDataSource.DATE_COL] = data[BaseDataSource.DATE_COL].dt.date
                data[data_source.get_columns()] = None

            new_data = data if new_data.empty else pd.merge(new_data, data, how="outer", on=BaseDataSource.DATE_COL)

        new_data[BaseDataSource.ERA_COL] = new_data[BaseDataSource.DATE_COL].apply(date_utils.get_era_for_date).astype(str).str.zfill(4)
        new_data = new_data.fillna(method="ffill")
        new_data = new_data.drop_duplicates(subset=[BaseDataSource.ERA_COL], keep="last")
        new_data = new_data.reindex(columns=[BaseDataSource.ERA_COL] 
                                    + new_data.columns.difference([BaseDataSource.ERA_COL]).tolist())
        new_data = new_data.drop(columns=[BaseDataSource.DATE_COL])
        self.data_cache = new_data.reset_index(drop=True)

        # write cache to disk
        self.data_cache.to_parquet(self.DATA_CACHE_FILE)

    def update_daily_data(self):
        new_data = pd.DataFrame()
        start_date = date_utils.get_current_date()
        end_date = date_utils.get_current_date()

        for data_source_class in self._get_data_sources():
            data_source = data_source_class()            
            try:
                data = data_source.get_data(start_date, end_date)
            except Exception as e:
                logging.exception(
                    f"Error getting data from {data_source_class.__name__}: {e} on {start_date} to {end_date}"
                )
                # fill with the last era value
                data = pd.DataFrame()
                data[BaseDataSource.DATE_COL] = pd.date_range(start_date, end_date)
                data[BaseDataSource.DATE_COL] = data[BaseDataSource.DATE_COL].dt.date
                data[data_source.get_columns()] = self.data_cache[data_source.get_columns()].tail(1).values

            new_data = data if new_data.empty else pd.merge(new_data, data, how="outer", on=BaseDataSource.DATE_COL)

        # add era column with X value so it can be merged with the live data
        new_data[BaseDataSource.ERA_COL] = "X"
        self.daily_cache = new_data
        self.daily_cache.to_parquet(self.DAILY_CACHE_FILE)

    def _get_data_sources(self) -> list:
        if len(self.class_cache) > 0:
            return self.class_cache

        full_subpackage_name = "numerai_era_data.data_sources"
        module = importlib.import_module(full_subpackage_name)
        classes = []

        for _, name, _ in pkgutil.iter_modules(module.__path__):
            sub_module = importlib.import_module(f"{full_subpackage_name}.{name}")
            for _, obj in inspect.getmembers(sub_module):
                if (
                    inspect.isclass(obj)
                    and inspect.getmodule(obj) == sub_module
                    and obj != BaseDataSource
                    and issubclass(obj, BaseDataSource)
                ):
                    classes.append(obj)

        self.class_cache = classes
        return classes
