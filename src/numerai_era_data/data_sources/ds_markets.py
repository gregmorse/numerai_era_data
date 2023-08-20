from datetime import date, datetime, timedelta

import pandas as pd
import pytz
import yfinance as yf

from numerai_era_data.data_sources.base_data_source import BaseDataSource


class DataSourceMarkets(BaseDataSource):
    _PREFIX = BaseDataSource._BASE_PREFIX + "markets_"
    _PREFIX_RAW = BaseDataSource._BASE_PREFIX_RAW + "markets_"
    _PREFIX_SPX_SMA = _PREFIX_RAW + "spx_sma_"
    _PREFIX_SPX_EMA = _PREFIX_RAW + "spx_ema_"
    _PREFIX_SPX_RETURN = _PREFIX + "spx_return_"
    _TIME_WINDOWS = [10, 20, 50, 100, 200]

    # columns
    COLUMN_SPX_CLOSE = _PREFIX_RAW + "spx_close"

    def __init__(self):
        self.COLUMNS = [self.COLUMN_SPX_CLOSE]
        
        for i in self._TIME_WINDOWS:
            setattr(self, f"COLUMN_SPX_SMA{i}", self._PREFIX_SPX_SMA + str(i))
            self.COLUMNS.append(getattr(self, f"COLUMN_SPX_SMA{i}"))
        for i in self._TIME_WINDOWS:
            setattr(self, f"COLUMN_SPX_EMA{i}", self._PREFIX_SPX_EMA + str(i))
            self.COLUMNS.append(getattr(self, f"COLUMN_SPX_EMA{i}"))
        for i in self._TIME_WINDOWS:
            setattr(self, f"COLUMN_SPX_RETURN{i}", self._PREFIX_SPX_RETURN + str(i))
            self.COLUMNS.append(getattr(self, f"COLUMN_SPX_RETURN{i}"))

    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        # adjusted close is more accurate than close
        CLOSE_COL = "Adj Close"

        # get 300 calendar days of padding for the 200 day moving average calculation
        padded_start_date = start_date - timedelta(days=300)

        # dataframe with all dates including weekends and holidays
        date_df = pd.DataFrame()
        date_df[self.DATE_COL] = pd.date_range(padded_start_date, end_date)
        date_df[self.DATE_COL] = date_df[self.DATE_COL].dt.date

        # dataframe with only trading days
        data = yf.download("^SPX", start=padded_start_date, end=end_date)
        data = data.reset_index()

        # calculate moving averages
        for i in self._TIME_WINDOWS:
            data[getattr(self, f"COLUMN_SPX_SMA{i}")] = data[CLOSE_COL].rolling(window=i).mean()

        # calculate exponential moving averages
        for i in self._TIME_WINDOWS:
            data[getattr(self, f"COLUMN_SPX_EMA{i}")] = data[CLOSE_COL].ewm(span=i, adjust=False).mean()

        # calculate returns
        for i in self._TIME_WINDOWS:
            data[getattr(self, f"COLUMN_SPX_RETURN{i}")] = data[CLOSE_COL].pct_change(periods=i)

        data.rename(columns={"Date": self.DATE_COL, CLOSE_COL: self.COLUMN_SPX_CLOSE}, inplace=True)

        # data is not finalized until around midnight Eastern time
        # add one day to date column to align with data availability
        data[self.DATE_COL] = data[self.DATE_COL].dt.date + timedelta(days=1)

        # merge market data with date data to fill in missing dates
        data = pd.merge(date_df, data, on=self.DATE_COL, how="left").ffill()

        # remove any data corresponding to future date (in eastern tz) as it may not be complete
        data = data[data[self.DATE_COL] <= datetime.now(pytz.timezone("US/Eastern")).date()]

        # filter out data outside of the requested date range
        data = data[(data[self.DATE_COL] >= start_date) & (data[self.DATE_COL] <= end_date)]

        # filter columns
        final_columns = [self.DATE_COL] + self.get_columns()
        data = data[final_columns]

        return data

    def get_columns(self) -> list:
        return self.COLUMNS
