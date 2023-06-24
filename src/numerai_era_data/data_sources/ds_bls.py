from datetime import date, timedelta

import pandas as pd
import requests

from numerai_era_data.data_sources.base_data_source import BaseDataSource


class DataSourceBLS(BaseDataSource):
    _PREFIX = BaseDataSource._BASE_PREFIX + "bls_"

    # columns
    COLUMN_CPI_U = _PREFIX + "cpi_u"
    COLUMN_CPI_U_ALL = _PREFIX + "cpi_u_all"
    COLUMN_PPI = _PREFIX + "ppi"
    COLUMN_PPI_ALL = _PREFIX + "ppi_all"
    COLUMN_PPI_FINISHED = _PREFIX + "ppi_finished"
    COLUMN_UE = _PREFIX + "unemployment"
    COLUMN_WEEKLY_HOURS = _PREFIX + "weekly_hours"
    COLUMN_HOURLY_EARNINGS = _PREFIX + "hourly_earnings"
    COLUMN_OUTPUT = _PREFIX + "output"
    COLUMNS = [COLUMN_CPI_U, COLUMN_CPI_U_ALL, COLUMN_PPI, COLUMN_PPI_ALL, COLUMN_PPI_FINISHED, COLUMN_UE,
               COLUMN_WEEKLY_HOURS, COLUMN_HOURLY_EARNINGS, COLUMN_OUTPUT]

    # BLS series IDs
    SERIES_ID_CPI_U = "CUUR0000SA0L1E"
    SERIES_ID_CPI_U_ALL_ITEMS = "CUUR0000SA0"
    SERIES_ID_PPI = "WPSFD49104"
    SERIES_ID_PPI_ALL_ITEMS = "WPSFD4"
    SERIES_ID_PPI_FINISHED_GOODS = "WPUFD49207"
    SERIES_ID_UNEMPLOYMENT = "LNS14000000"
    SERIES_ID_WEEKLY_HOURS = "CES0500000002"
    SERIES_ID_HOURLY_EARNINGS = "CES0500000003"
    SERIES_ID_OUTPUT = "PRS85006092"

    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        # add six months of padding to the start date
        padded_start_date = start_date - timedelta(days=184)
        
        date_df = pd.DataFrame()
        date_df[self.DATE_COL] = pd.date_range(padded_start_date, end_date)
        date_df[self.DATE_COL] = date_df[self.DATE_COL].dt.date
        
        # Define the URL for the BLS API
        api_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

        # Define the BLS API headers
        headers = {'Content-type': 'application/json'}

        # Define the BLS API request data
        series_ids = [self.SERIES_ID_CPI_U,
                      self.SERIES_ID_CPI_U_ALL_ITEMS,
                      self.SERIES_ID_PPI,
                      self.SERIES_ID_PPI_ALL_ITEMS,
                      self.SERIES_ID_PPI_FINISHED_GOODS,
                      self.SERIES_ID_UNEMPLOYMENT,
                      self.SERIES_ID_WEEKLY_HOURS,
                      self.SERIES_ID_HOURLY_EARNINGS,
                      self.SERIES_ID_OUTPUT]

        data = []

        request_data = {
            "seriesid": series_ids,
            "startyear": str(padded_start_date.year),
            "endyear": str(end_date.year)
        }

        # Send request to the BLS API
        response = requests.post(api_url, headers=headers, json=request_data)

        # Check if the request was successful
        if response.status_code == 200:
            json_response = response.json()
            data = []

            for series in json_response['Results']['series']:
                series_id = series['seriesID']
                series_data = series['data']

                # Extract values and dates
                values = [float(data_point['value']) for data_point in series_data]
                dates = []
                for data_point in series_data:                    
                    if data_point['period'][0] == 'Q':
                        quarter_num = int(data_point['period'][1:])
                        year = int(data_point['year'])
                        month = (quarter_num - 1) * 3 + 1
                        date = pd.to_datetime(f'{year}-{month}-01')
                    else:
                        date = pd.to_datetime(data_point['year'] + '-' + data_point['period'][1:])
                    dates.append(date)

                # Create DataFrame for the series data
                series_df = pd.DataFrame({'Date': dates, series_id: values})
                series_df = series_df.set_index('Date')

                data.append(series_df)

            # Combine the data into a single DataFrame
            combined_df = pd.concat(data, axis=1).reset_index()
        else:
            print(f"Error occurred while fetching data for series ID: {series_id}")
            raise Exception(response.text)

        # rename columns
        combined_df.rename(columns={"Date": self.DATE_COL,
                                    self.SERIES_ID_CPI_U: self.COLUMN_CPI_U,
                                    self.SERIES_ID_CPI_U_ALL_ITEMS: self.COLUMN_CPI_U_ALL,
                                    self.SERIES_ID_PPI: self.COLUMN_PPI,
                                    self.SERIES_ID_PPI_ALL_ITEMS: self.COLUMN_PPI_ALL,
                                    self.SERIES_ID_PPI_FINISHED_GOODS: self.COLUMN_PPI_FINISHED,
                                    self.SERIES_ID_UNEMPLOYMENT: self.COLUMN_UE,
                                    self.SERIES_ID_WEEKLY_HOURS: self.COLUMN_WEEKLY_HOURS,
                                    self.SERIES_ID_HOURLY_EARNINGS: self.COLUMN_HOURLY_EARNINGS,
                                    self.SERIES_ID_OUTPUT: self.COLUMN_OUTPUT}, inplace=True)

        # convert date column to datetime
        combined_df[self.DATE_COL] = pd.to_datetime(combined_df[self.DATE_COL])     
        combined_df[self.DATE_COL] = combined_df[self.DATE_COL].dt.date

        # merge market data with date data to fill in missing dates
        data = pd.merge(date_df, combined_df, on=self.DATE_COL, how="left").ffill()

        # shift each column based on its release schedule   
        data[self.COLUMN_CPI_U] = data[self.COLUMN_CPI_U].shift(48)
        data[self.COLUMN_CPI_U_ALL] = data[self.COLUMN_CPI_U_ALL].shift(48)
        data[self.COLUMN_PPI] = data[self.COLUMN_PPI].shift(45)
        data[self.COLUMN_PPI_ALL] = data[self.COLUMN_PPI_ALL].shift(45)
        data[self.COLUMN_PPI_FINISHED] = data[self.COLUMN_PPI_FINISHED].shift(45)
        data[self.COLUMN_UE] = data[self.COLUMN_UE].shift(38)
        data[self.COLUMN_WEEKLY_HOURS] = data[self.COLUMN_WEEKLY_HOURS].shift(38)
        data[self.COLUMN_HOURLY_EARNINGS] = data[self.COLUMN_HOURLY_EARNINGS].shift(38)
        data[self.COLUMN_OUTPUT] = data[self.COLUMN_OUTPUT].shift(134)

        data = data[(data[self.DATE_COL] >= start_date)]

        return data

    def get_columns(self) -> list:
        return self.COLUMNS
