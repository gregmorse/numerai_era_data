from datetime import date

import pandas as pd

from numerai_era_data.data_sources.base_data_source import BaseDataSource


class DataSourceCalendar(BaseDataSource):
    _PREFIX = BaseDataSource._BASE_PREFIX + "calendar_"

    # columns
    COLUMN_MONTH = _PREFIX + "month"
    COLUMN_QUARTER = _PREFIX + "quarter"
    COLUMN_YEAR = _PREFIX + "year"
    COLUMNS = [COLUMN_MONTH, COLUMN_QUARTER, COLUMN_YEAR]

    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        data = pd.DataFrame()
        data[self.DATE_COL] = pd.date_range(start_date, end_date)
        data[self.COLUMN_MONTH] = data[self.DATE_COL].dt.month
        data[self.COLUMN_QUARTER] = data[self.DATE_COL].dt.quarter
        data[self.COLUMN_YEAR] = data[self.DATE_COL].dt.year
        data[self.DATE_COL] = data[self.DATE_COL].dt.date

        return data

    def get_columns(self) -> list:
        return self.COLUMNS
