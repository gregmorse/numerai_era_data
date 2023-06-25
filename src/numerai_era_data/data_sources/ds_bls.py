from datetime import date, timedelta

import pandas as pd
import requests

from numerai_era_data.data_sources.base_data_source import BaseDataSource


class DataSourceBLS(BaseDataSource):
    _PREFIX = BaseDataSource._BASE_PREFIX + "bls_"
    _PREFIX_RAW = BaseDataSource._BASE_PREFIX_RAW + "bls_"

    # columns
    COLUMN_CPI_U = _PREFIX_RAW + "cpi_u"
    COLUMN_CPI_U_ALL = _PREFIX_RAW + "cpi_u_all"
    COLUMN_PPI_FINISHED = _PREFIX_RAW + "ppi_finished"
    COLUMN_UE = _PREFIX + "unemployment"
    COLUMN_WEEKLY_HOURS = _PREFIX + "weekly_hours"
    COLUMN_HOURLY_EARNINGS = _PREFIX_RAW + "hourly_earnings"
    COLUMN_OUTPUT = _PREFIX + "output"
    COLUMN_IMPORT_INDEX = _PREFIX_RAW + "import_index"
    COLUMN_EXPORT_INDEX = _PREFIX_RAW + "export_index"

    # columns from month over month and year over year changes
    COLUMN_CPI_U_MOM = _PREFIX + "cpi_u_mom"
    COLUMN_CPI_U_YOY = _PREFIX + "cpi_u_yoy"
    COLUMN_CPI_U_ALL_MOM = _PREFIX + "cpi_u_all_mom"
    COLUMN_CPI_U_ALL_YOY = _PREFIX + "cpi_u_all_yoy"
    COLUMN_PPI_FINISHED_MOM = _PREFIX + "ppi_finished_mom"
    COLUMN_PPI_FINISHED_YOY = _PREFIX + "ppi_finished_yoy"
    COLUMN_UE_MOM = _PREFIX + "unemployment_mom"
    COLUMN_UE_YOY = _PREFIX + "unemployment_yoy"
    COLUMN_WEEKLY_HOURS_MOM = _PREFIX + "weekly_hours_mom"
    COLUMN_WEEKLY_HOURS_YOY = _PREFIX + "weekly_hours_yoy"
    COLUMN_HOURLY_EARNINGS_MOM = _PREFIX + "hourly_earnings_mom"
    COLUMN_HOURLY_EARNINGS_YOY = _PREFIX + "hourly_earnings_yoy"
    COLUMN_OUTPUT_MOM = _PREFIX + "output_mom"
    COLUMN_OUTPUT_YOY = _PREFIX + "output_yoy"
    COLUMN_IMPORT_INDEX_MOM = _PREFIX + "import_index_mom"
    COLUMN_IMPORT_INDEX_YOY = _PREFIX + "import_index_yoy"
    COLUMN_EXPORT_INDEX_MOM = _PREFIX + "export_index_mom"
    COLUMN_EXPORT_INDEX_YOY = _PREFIX + "export_index_yoy"

    COLUMNS = [
        COLUMN_CPI_U,
        COLUMN_CPI_U_ALL,
        COLUMN_PPI_FINISHED,
        COLUMN_UE,
        COLUMN_WEEKLY_HOURS,
        COLUMN_HOURLY_EARNINGS,
        COLUMN_OUTPUT,
        COLUMN_IMPORT_INDEX,
        COLUMN_EXPORT_INDEX,
        COLUMN_CPI_U_MOM,
        COLUMN_CPI_U_YOY,
        COLUMN_CPI_U_ALL_MOM,
        COLUMN_CPI_U_ALL_YOY,
        COLUMN_PPI_FINISHED_MOM,
        COLUMN_PPI_FINISHED_YOY,
        COLUMN_UE_MOM,
        COLUMN_UE_YOY,
        COLUMN_WEEKLY_HOURS_MOM,
        COLUMN_WEEKLY_HOURS_YOY,
        COLUMN_HOURLY_EARNINGS_MOM,
        COLUMN_HOURLY_EARNINGS_YOY,
        COLUMN_OUTPUT_MOM,
        COLUMN_OUTPUT_YOY,
        COLUMN_IMPORT_INDEX_MOM,
        COLUMN_IMPORT_INDEX_YOY,
        COLUMN_EXPORT_INDEX_MOM,
        COLUMN_EXPORT_INDEX_YOY,
    ]

    # BLS series IDs
    SERIES_ID_CPI_U = "CUUR0000SA0L1E"
    SERIES_ID_CPI_U_ALL_ITEMS = "CUUR0000SA0"
    SERIES_ID_PPI_FINISHED_GOODS = "WPUFD49207"
    SERIES_ID_UNEMPLOYMENT = "LNS14000000"
    SERIES_ID_WEEKLY_HOURS = "CES0500000002"
    SERIES_ID_HOURLY_EARNINGS = "CES0500000003"
    SERIES_ID_OUTPUT = "PRS85006092"
    SERIES_ID_IMPORT_INDEX = "EIUIR"
    SERIES_ID_EXPORT_INDEX = "EIUIQ"

    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        # add 18 months of padding to the start date
        # accounts for delays in reporting and need to calculate 12 month changes
        padded_start_date = start_date - timedelta(days=549)

        date_df = pd.DataFrame()
        date_df[self.DATE_COL] = pd.date_range(padded_start_date, end_date)
        date_df[self.DATE_COL] = date_df[self.DATE_COL].dt.date

        # Define the URL for the BLS API
        api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

        # Define the BLS API headers
        headers = {"Content-type": "application/json"}

        # Define the BLS API request data
        series_ids = [
            self.SERIES_ID_CPI_U,
            self.SERIES_ID_CPI_U_ALL_ITEMS,
            self.SERIES_ID_PPI_FINISHED_GOODS,
            self.SERIES_ID_UNEMPLOYMENT,
            self.SERIES_ID_WEEKLY_HOURS,
            self.SERIES_ID_HOURLY_EARNINGS,
            self.SERIES_ID_OUTPUT,
            self.SERIES_ID_IMPORT_INDEX,
            self.SERIES_ID_EXPORT_INDEX,
        ]

        data = []

        request_data = {
            "seriesid": series_ids,
            "startyear": str(padded_start_date.year),
            "endyear": str(end_date.year),
        }

        # Send request to the BLS API
        response = requests.post(api_url, headers=headers, json=request_data)

        # Check if the request was successful
        if response.status_code == 200:
            json_response = response.json()
            data = []

            for series in json_response["Results"]["series"]:
                series_id = series["seriesID"]
                series_data = series["data"]

                # Extract values and dates
                values = [float(data_point["value"]) for data_point in series_data]
                dates = []
                for data_point in series_data:
                    if data_point["period"][0] == "Q":
                        quarter_num = int(data_point["period"][1:])
                        year = int(data_point["year"])
                        month = (quarter_num - 1) * 3 + 1
                        date = pd.to_datetime(f"{year}-{month}-01")
                    else:
                        date = pd.to_datetime(data_point["year"] + "-" + data_point["period"][1:])
                    dates.append(date)

                # Create DataFrame for the series data
                series_df = pd.DataFrame({"Date": dates, series_id: values})
                series_df = series_df.set_index("Date")

                data.append(series_df)

            # Combine the data into a single DataFrame
            combined_df = pd.concat(data, axis=1).reset_index()
        else:
            print(f"Error occurred while fetching data for series ID: {series_id}")
            raise Exception(response.text)

        # rename columns
        combined_df.rename(
            columns={
                "Date": self.DATE_COL,
                self.SERIES_ID_CPI_U: self.COLUMN_CPI_U,
                self.SERIES_ID_CPI_U_ALL_ITEMS: self.COLUMN_CPI_U_ALL,
                self.SERIES_ID_PPI_FINISHED_GOODS: self.COLUMN_PPI_FINISHED,
                self.SERIES_ID_UNEMPLOYMENT: self.COLUMN_UE,
                self.SERIES_ID_WEEKLY_HOURS: self.COLUMN_WEEKLY_HOURS,
                self.SERIES_ID_HOURLY_EARNINGS: self.COLUMN_HOURLY_EARNINGS,
                self.SERIES_ID_OUTPUT: self.COLUMN_OUTPUT,
                self.SERIES_ID_IMPORT_INDEX: self.COLUMN_IMPORT_INDEX,
                self.SERIES_ID_EXPORT_INDEX: self.COLUMN_EXPORT_INDEX,
            },
            inplace=True,
        )

        # calculate month-over-month and year-over-year changes
        combined_df[self.COLUMN_CPI_U_MOM] = combined_df[self.COLUMN_CPI_U].pct_change(1)
        combined_df[self.COLUMN_CPI_U_YOY] = combined_df[self.COLUMN_CPI_U].pct_change(12)
        combined_df[self.COLUMN_CPI_U_ALL_MOM] = combined_df[self.COLUMN_CPI_U_ALL].pct_change(1)
        combined_df[self.COLUMN_CPI_U_ALL_YOY] = combined_df[self.COLUMN_CPI_U_ALL].pct_change(12)
        combined_df[self.COLUMN_PPI_FINISHED_MOM] = combined_df[self.COLUMN_PPI_FINISHED].pct_change(1)
        combined_df[self.COLUMN_PPI_FINISHED_YOY] = combined_df[self.COLUMN_PPI_FINISHED].pct_change(12)
        combined_df[self.COLUMN_UE_MOM] = combined_df[self.COLUMN_UE].pct_change(1)
        combined_df[self.COLUMN_UE_YOY] = combined_df[self.COLUMN_UE].pct_change(12)
        combined_df[self.COLUMN_WEEKLY_HOURS_MOM] = combined_df[self.COLUMN_WEEKLY_HOURS].pct_change(1)
        combined_df[self.COLUMN_WEEKLY_HOURS_YOY] = combined_df[self.COLUMN_WEEKLY_HOURS].pct_change(12)
        combined_df[self.COLUMN_HOURLY_EARNINGS_MOM] = combined_df[self.COLUMN_HOURLY_EARNINGS].pct_change(1)
        combined_df[self.COLUMN_HOURLY_EARNINGS_YOY] = combined_df[self.COLUMN_HOURLY_EARNINGS].pct_change(12)
        combined_df[self.COLUMN_OUTPUT_MOM] = combined_df[self.COLUMN_OUTPUT].pct_change(1)
        combined_df[self.COLUMN_OUTPUT_YOY] = combined_df[self.COLUMN_OUTPUT].pct_change(12)
        combined_df[self.COLUMN_IMPORT_INDEX_MOM] = combined_df[self.COLUMN_IMPORT_INDEX].pct_change(1)
        combined_df[self.COLUMN_IMPORT_INDEX_YOY] = combined_df[self.COLUMN_IMPORT_INDEX].pct_change(12)
        combined_df[self.COLUMN_EXPORT_INDEX_MOM] = combined_df[self.COLUMN_EXPORT_INDEX].pct_change(1)
        combined_df[self.COLUMN_EXPORT_INDEX_YOY] = combined_df[self.COLUMN_EXPORT_INDEX].pct_change(12)

        # convert date column to datetime
        combined_df[self.DATE_COL] = pd.to_datetime(combined_df[self.DATE_COL])
        combined_df[self.DATE_COL] = combined_df[self.DATE_COL].dt.date

        # merge market data with date data to fill in missing dates
        data = pd.merge(date_df, combined_df, on=self.DATE_COL, how="left").ffill()

        # shift each column based on its release schedule
        data[self.COLUMN_CPI_U] = data[self.COLUMN_CPI_U].shift(48)
        data[self.COLUMN_CPI_U_ALL] = data[self.COLUMN_CPI_U_ALL].shift(48)
        data[self.COLUMN_PPI_FINISHED] = data[self.COLUMN_PPI_FINISHED].shift(45)
        data[self.COLUMN_UE] = data[self.COLUMN_UE].shift(38)
        data[self.COLUMN_WEEKLY_HOURS] = data[self.COLUMN_WEEKLY_HOURS].shift(38)
        data[self.COLUMN_HOURLY_EARNINGS] = data[self.COLUMN_HOURLY_EARNINGS].shift(38)
        data[self.COLUMN_OUTPUT] = data[self.COLUMN_OUTPUT].shift(134)
        data[self.COLUMN_IMPORT_INDEX] = data[self.COLUMN_IMPORT_INDEX].shift(45)
        data[self.COLUMN_EXPORT_INDEX] = data[self.COLUMN_EXPORT_INDEX].shift(45)

        data = data[(data[self.DATE_COL] >= start_date)][[self.DATE_COL] + self.COLUMNS]

        return data

    def get_columns(self) -> list:
        return self.COLUMNS
