from datetime import date, timedelta

import pandas as pd
import requests

from numerai_era_data.data_sources.base_data_source import BaseDataSource


class DataSourceWEI(BaseDataSource):
    _PREFIX = BaseDataSource._BASE_PREFIX + "wei_"

    # columns
    COLUMN_WEI = _PREFIX + "wei"
    COLUMNS = [COLUMN_WEI]

    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        # add 13 days of padding
        padded_start_date = start_date - timedelta(days=13)

        date_df = pd.DataFrame()
        date_df[self.DATE_COL] = pd.date_range(padded_start_date, end_date)
        date_df[self.DATE_COL] = date_df[self.DATE_COL].dt.date

        # URL of the weekly economic index data
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WEI"

        # Make the HTTP request to fetch the data
        response = requests.get(url)
        response.raise_for_status()

        # Create a DataFrame from the CSV data
        wei_df = pd.read_csv(url)

        # rename columns
        wei_df.rename(columns={"DATE": self.DATE_COL, "WEI": self.COLUMN_WEI}, inplace=True)

        # convert date column to datetime
        wei_df[self.DATE_COL] = pd.to_datetime(wei_df[self.DATE_COL])

        # data is not ready until after noon UTC on Thursday, dates are for previous Saturday
        wei_df[self.DATE_COL] = wei_df[self.DATE_COL].dt.date + timedelta(days=6)

        # merge market data with date data to fill in missing dates
        data = pd.merge(date_df, wei_df, on=self.DATE_COL, how="left").ffill()

        data = data[(data[self.DATE_COL] >= start_date)]

        return data

    def get_columns(self) -> list:
        return self.COLUMNS
