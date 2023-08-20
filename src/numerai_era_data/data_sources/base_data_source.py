from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class BaseDataSource(ABC):
    _BASE_PREFIX = "era_feature_"
    _BASE_PREFIX_RAW = "era_feature_raw_"
    DATE_COL = "date"
    ERA_COL = "era"

    @abstractmethod
    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:  # pragma: no cover
        """Returns a dataframe with the following columns:
        - date: datetime
        - one or more features
        feature data must correspond to data available by noon UTC on the given date
        start_date and end_date are inclusive"""
        pass

    @abstractmethod
    def get_columns(self) -> list:  # pragma: no cover
        pass
