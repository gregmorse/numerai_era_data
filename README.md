# Numerai Era Data

Numerai Era Data is a Python project dedicated to enriching the Numerai tournament experience by providing supplemental era-level data. This data may offer valuable insights to enhance the modeling capabilities of participants in the Numerai tournament.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Data Types](#data-types)
- [Extending Data Sources](#extending-data-sources)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The Numerai tournament provides an innovative platform for data scientists to build predictive models for financial data. Numerai Era Data takes this a step further by offering supplemental data at the era level. These data enhancements can be seamlessly integrated into participants' modeling pipelines, enabling them to explore new approaches and potentially improve their model performance.

Era-level data could be incorporated into a model pipeline in many ways.  The simplest approach would be to add the supplemental columns as new feature columns directly to the Numerai data.  Initial tests on this approach have not shown any benefit.  Another avenue would be to use the era-level data to help predict and respond to changes in regime, which has seemed to plague Numerai participants periodically, including during the heavy drawdowns of Q2 2023.  One approach along this avenue would be to cluster eras based on era-level feature similarity and then train a separate model or models on each cluster.  These models could then be used in an ensemble or mixture-of-experts system.

## Installation

To start utilizing Numerai Era Data, install it from PyPI using the following command:

```
pip install numerai-era-data
```

## Usage

Numerai Era Data can be incorporated into your Numerai modeling process to enhance your models' predictive power. Here's how you can use it:


```
from numerai_era_data.era_data_api import EraDataAPI

# Get data for all eras and latest daily data for live era
era_data_api = EraDataAPI()
era_data = era_data_api.get_all_eras()
daily_data = era_data_api.get_current_daily()

# Exclude raw columns
era_feature_columns = [f for f in era_data.columns if f != "era" and not f.startswith("era_feature_raw_")]

# Merge era data with Numerai data
all_data = all_data.merge(era_data[["era"] + era_feature_columns], on="era", how="left")
live_data = live_data.merge(daily_data[["era"] + era_feature_columns], on="era", how="outer")
```

## Data Types

Numerai Era Data provides two types of columns: normal and raw. Raw features, indicated by the prefix "era_feature_raw_", require additional processing to be useful in modeling. These features encompass data like the S&P500 closing price. Incorporating these columns can potentially contribute to more accurate and sophisticated models.
Extending Data Sources

## Contributing

Numerai Era Data welcomes contributors to expand its capabilities by implementing new data sources.  To add a new data source, follow these steps:

1. Create a new class that extends numerai_era_data.data_sources.base_data_source.BaseDataSource.
1. Implement the get_data() function in the new class, returning a Pandas DataFrame.  The DataFrame should have a "date" column and one or more columns starting with either "_BASE_PREFIX" or "_BASE_PREFIX_RAW". These columns should contain the values available at noon UTC for each date in the DataFrame's range.
1. Implement the get_columns() function to return the list of data columns provided by the new data source.

## License

Numerai Era Data is released under the MIT License. You are free to use, modify, and distribute the code according to the terms of the license.
