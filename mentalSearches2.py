import os
from datetime import datetime

import pandas as pd
import hdate

PROJECT_DIR = "/Users/urischwartz/PycharmProjects/mentalSearches/"
RAW_DATA_DIR = os.path.join(PROJECT_DIR, "rawData")
date_time_format = "%d/%m/%Y"
MENTAL_SEARCH_TREND_DATAFRAME_CSV_PATH = os.path.join(PROJECT_DIR, "mental_search_trend_dataframe.csv")

FILE_TO_DATES_DICT = {
    '2019B.csv': (datetime(2019, 7, 1), datetime(2019, 12, 31)),
    '2020A.csv': (datetime(2020, 1, 1), datetime(2020, 6, 30)),
    '2020B.csv': (datetime(2020, 7, 1), datetime(2021, 12, 31)),
    '2021A.csv': (datetime(2021, 1, 1), datetime(2021, 6, 30)),
    '2021B.csv': (datetime(2021, 7, 1), datetime(2022, 12, 31)),
    '2022A.csv': (datetime(2022, 1, 1), datetime(2022, 6, 30)),
    '2022B.csv': (datetime(2022, 7, 1), datetime(2022, 12, 31)),
    '2023A.csv': (datetime(2023, 1, 1), datetime(2023, 6, 30)),
    '2023B.csv': (datetime(2023, 7, 1), datetime(2023, 12, 31)),
    '2024A.csv': (datetime(2024, 1, 1), datetime(2024, 6, 30)),
}

jewish_holiday_hebrew_dates = [
    (1,1),    # Jewish New Year
    (1,2),    # Jewish New Year Day 2
    (1,10),   # Yom Kippur
    (1,15),   # Sukkot 1
    (7,15),   # Passover Eve
    (7,16),   # Passover Day
    (9,6),   # Shavuot
]

# List of Israeli national holidays
israeli_national_memorial_hebrew_dates = [
    (7, 27),  # Yom HaShoah (Holocaust Remembrance Day) - 27 Nisan, variable Gregorian date
    (8, 4),  # Yom HaZikaron (Memorial Day) - 4 Iyar, variable Gregorian date
]

# List of covid related quarantine periods in Israel
israeli_qurantine_ranges = [
    (datetime(2020, 3, 25), datetime(2020, 5, 4)),
    (datetime(2020, 9, 18), datetime(2020, 10, 17)),
    (datetime(2020, 12, 27), datetime(2021, 2, 7))
]

# Date of Oct 7th
oct_7_date = datetime(2023, 10, 7)

raw_data_file_list = [os.path.join(RAW_DATA_DIR, fn) for fn in FILE_TO_DATES_DICT.keys()]

def get_season_by_date(date: datetime):
    """
    This function takes a date and returns the name of the season it's in
    Fall = Sept through Nov
    Winter = Dec through Feb
    Spring = Mar through May
    Summer = Jun through Aug
    """
    month_dict = {
        1: "winter",
        2: "winter",
        3: "spring",
        4: "spring",
        5: "spring",
        6: "summer",
        7: "summer",
        8: "summer",
        9: "fall",
        10: "fall",
        11: "fall",
        12: "winter"
    }

    if date.month in month_dict:
        return month_dict[date.month]
    else:
        raise Exception(f"Month not found in month dict: {date}")


def is_jewish_holiday(date):
    hebrew_date = hdate.HDate(date)
    for holiday in jewish_holiday_hebrew_dates:
        if hebrew_date.hdate.month.value == holiday[0] and hebrew_date.hdate.day == holiday[1]:
            return True
    return False


def is_national_memorial_day(date):
    hebrew_date = hdate.HDate(date)
    for holiday in israeli_national_memorial_hebrew_dates:
        if hebrew_date.hdate.month.value == holiday[0] and hebrew_date.hdate.day == holiday[1]:
            return True
    return False


def is_date_in_ranges(date_to_check, date_ranges):
    """
    Check if a date falls within any of the date ranges provided.

    Parameters:
    - date_to_check (datetime.date): The date to check.
    - date_ranges (list of tuples): List of date ranges, each represented as (start_date, end_date).

    Returns:
    - bool: True if the date falls within any range, False otherwise.
    """
    for start_date, end_date in date_ranges:
        if start_date <= date_to_check <= end_date:
            return True
    return False


def modify_dataframe_row(original_row, date, multiplier):
    # Copy to new row
    modified_row = original_row.copy()
    # Save new date format
    modified_row[0] = date

    # Multiply all search values by multiplier
    for i in range(1, len(modified_row)):
        modified_row[i] = modified_row[i] * multiplier

    # Add average info search rate to output row
    average_info_search_rate = sum(modified_row[1:10]) / len(modified_row[1:10])
    modified_row["average_info_search_rate"] = average_info_search_rate

    # Add average help search rate to output row
    average_help_search_rate = sum(modified_row[10:15]) / len(modified_row[10:15])
    modified_row["average_help_search_rate"] = average_help_search_rate

    # Add average total search rate to output row
    average_total_search_rate = sum(modified_row[1:15]) / len(modified_row[1:15])
    modified_row["average_total_search_rate"] = average_total_search_rate

    # Add multiplier value column
    modified_row["multiplier"] = multiplier

    # Add month value column
    modified_row["month"] = date.month

    # Add season column
    modified_row["season"] = get_season_by_date(date)

    # Check if weekend and add bool value
    modified_row["is_weekend"] = date.weekday() in [4,5] # Monday = 0... Weekend is 4,5

    # Check if jewish holiday and add bool value
    modified_row["is_jewish_holiday"] = is_jewish_holiday(date)

    # Check if national memorial day and add bool value
    modified_row["is_national_memorial_day"] = is_national_memorial_day(date)

    # Check if quarantine and add bool value
    modified_row["is_quarantine"] = is_date_in_ranges(date, israeli_qurantine_ranges)

    # Check if after Oct 7 and add bool value
    modified_row["is_after_oct_7"] = bool(date >= oct_7_date)

    return modified_row


def create_search_trend_dataframe(raw_data_file_list: str) -> pd.DataFrame:
    # Create output dataframe
    modified_rows = []
    multiplier = 1
    multiplier_row = []
    col_index = 0

    # For each raw data file in raw data file list
    for raw_data_file in raw_data_file_list:
        # Read the raw data from the data file list
        raw_dataframe = pd.read_csv(raw_data_file, skiprows=2)
        if len(multiplier_row) and col_index:
            matching_row = raw_dataframe[raw_dataframe["Day"] == multiplier_row["Day"]]
            if len(matching_row) > 0:
                multiplier = multiplier_row[col_index] / matching_row.iloc[0][col_index]
                multiplier_row = []
            else:
                raise Exception("Matching row not found, unable to convert search values between different queries")

        # For each row in raw data frame, extract data from raw dataframe and put into output trend dataframe
        for index, row in raw_dataframe.iterrows():
            # Convert date to datetime format
            date = datetime.strptime(row[0], date_time_format)

            # Check if date in range (or there only for normalizing values)
            date_range = FILE_TO_DATES_DICT[os.path.basename(raw_data_file)]
            if date_range[0] <= date <= date_range[1]:

                # modify the row according to what we need and add to dataframe
                modified_row = modify_dataframe_row(original_row=row, date=date, multiplier=multiplier)

                # Add row to modified row list
                modified_rows.append(modified_row)

            elif date > date_range[1] and not len(multiplier_row):
                for i in range(1, len(row)):
                    if bool(row[i]):
                        multiplier_row = row
                        col_index = i
                        break

    search_trend_dataframe = pd.DataFrame(modified_rows)

    return search_trend_dataframe


if __name__ == "__main__":
    # Read raw general search files, create search trend dataframe and export to CSV
    search_trend_dataframe = create_search_trend_dataframe(raw_data_file_list)
    search_trend_dataframe.to_csv(MENTAL_SEARCH_TREND_DATAFRAME_CSV_PATH)