from os import remove
import requests
from urllib.parse import quote, urlencode, urlparse
import datetime, calendar
import json
from requests.exceptions import ReadTimeout
from historical_derivatives_bhav import get_headers
import pickle
from zipfile import BadZipFile, ZipFile
from pathlib import Path
from requests.exceptions import ReadTimeout
import os
import itertools
import pandas as pd


# Data Mining of NIFTY options from NSE Website and analysis in Pandas Library


def get_data():

# makes csv files in ./derivaties/__year_name__/
    """
    change year and month to desired
    and change cookie to a fresh cookie by sniffing the request header in chrome network panel
    """
# change the below lines to get different years' diff months data like this -
# for years 2021 and 2022's data, this can be done ->  yr = ['2021', '2022']
# for selecting only jan and feb, this can be done ->  mnth = ['JAN','FEB']
#yr = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]

    yr = [2012]
#mnth = [0] # all

    mnth = [4, 5, 6, 7, 8, 9, 10, 11, 12] # some

    BASE_DIR = Path(__file__).resolve().parent
    derivatives = BASE_DIR / 'derivatives'

    alldates = []
    for each_year in yr:
        yr_path = derivatives / str(each_year)
        yr_path.mkdir(parents=True, exist_ok=True)
        if mnth == [0]:
            for each_mnth in range(1, 13):
                num_days = calendar.monthrange(each_year, each_mnth)[1]
                alldates += [datetime.date(each_year, each_mnth, day) for day in range(1, num_days+1)]
        else:
            for each_mnth in mnth:
                num_days = calendar.monthrange(each_year, each_mnth)[1]
                alldates += [datetime.date(each_year, each_mnth, day) for day in range(1, num_days+1)]


    derivatives.mkdir(parents=True, exist_ok=True)

    base_url = 'https://www.nseindia.com/api/reports'

    for day in alldates:

        cur_year = str(day.year)
        cur_date = ""
        if day.day < 10:
            cur_date += "0"
        cur_date += str(day.day)
        cur_month = day.strftime("%c").split()[1]

        params = {
                'archives': '[{"name":"F&O - Bhavcopy(csv)","type":"archives","category":"derivatives","section":"equity"}]',
                'date': '{}-{}-{}'.format(cur_date, cur_month, cur_year),
                'type': 'equity',
                'mode': 'single'
                }

        print("downloading day {} rn".format(day.isoformat()))
        cur_year_path = derivatives / cur_year

        day_zipfile_path = cur_year_path / 'day{}.zip'.format(day.isoformat())
        try:
            response = requests.get(base_url, timeout=5, stream = True, headers= get_headers(base_url, params, custom_cookie=None), params= params)
            print("final request was - ", response.url)
            #response = requests.get(st1, allow_redirects=True, timeout=5)
            with open(day_zipfile_path, 'wb') as ziphandle:
                ziphandle.write(response.content)
        except ReadTimeout:
            print("\ttimed out\n")
            continue

        with open(day_zipfile_path, 'rb') as f:
            try:
                z = ZipFile(f)
                z.extractall(path = cur_year_path)
            except BadZipFile:
                print("\tbad zip file\n")
        remove(day_zipfile_path)


def black_scholes_formula(df_row):

    # takes in a dataframe row, gives out the theoretical price of option

    option_is_call = df_row['OPTION_TYP'] == 'CE'
    rate = df_row['T_BILL_RATE'] / 100
    expiry_years_delta = (df_row['EXPIRY_DT'] - df_row['BUY_DT']).days / 365
    spot_pr = df_row['SPOT_PR']
    strike_pr = df_row['STRIKE_PR']
    
    if expiry_years_delta == 0 and option_is_call:
        return spot_pr - strike_pr
    elif expiry_years_delta == 0:
        return strike_pr - spot_pr
        
    discount_factor = exp(- rate * expiry_years_delta)
    d1 = (log(spot_pr / strike_pr) + (rate + (vol**2)/2) * expiry_years_delta) / vol * sqrt(expiry_years_delta)
    d2 = d1 - vol * sqrt(expiry_years_delta)
    if option_is_call:
        return spot_pr * norm.cdf(d1) - strike_pr * discount_factor * norm.cdf(d2)
    else:
        return  norm.cdf(- d2) * strike_pr * discount_factor - spot_pr * norm.cdf(- d1)



def lower_boundary_condition_evaluator(df_row):

    # takes in the dataframe row (option)
    # returns if the lower boundary condition is satisfied

    option_is_call = df_row['OPTION_TYP'] == 'CE'
    theoretical_price_val = theoretical_price(df_row)
    if option_is_call:
        intrinsic_value = df_row['SPOT_PR'] - df_row['STRIKE_PR']
        return theoretical_price_val <= intrinsic_value
    else:
        intrinsic_value = df_row['STRIKE_PR'] - df_row['SPOT_PR']
        return theoretical_price_val >= intrinsic_value



def clean_data(year_to_clean):

    # Initialize an empty list to store the dataframes
    df_list = []

    fc1 = BASE_DIR / "options_{}.csv".format(year_to_clean);
    if(fc1.exists()):
        print("year cleaning already done");
        return;
    
    data_path_to_clean = BASE_DIR / "derivatives" / year_to_clean
    # Iterate through the files in the folder and its subfolders
    for root, dirs, files in os.walk(data_path_to_clean):
        for file in files:
            # Only consider CSV files
            if file.endswith(".csv"):
                # Read the CSV file into a dataframe
                df = pd.read_csv(os.path.join(root, file))
                # Add the dataframe to the list
                df_list.append(df)
    # Concatenate all the dataframes into a single dataframe
    result = pd.concat(df_list)

    print("concatted df is: ")
    print(result)

    # drop columns if name contains 'Unnamed'
    # other preprocessing
    result = result.loc[:, ~result.columns.str.contains('^Unnamed')]
    result['EXPIRY_DT'] = pd.to_datetime(result['EXPIRY_DT'], format="%d-%b-%Y")
    result['TIMESTAMP'] = pd.to_datetime(result['TIMESTAMP'], format="%d-%b-%Y")
    delta_series = result['EXPIRY_DT'] - result['TIMESTAMP']
    result['time_to_expiration'] = delta_series.dt.days

    result.to_csv("cached_data_{}.csv".format(year_to_clean))


    # getting nifty options and tagging them
    nifty = result[(result['SYMBOL']=='NIFTY') & (result['INSTRUMENT']=='OPTIDX') ]

    option_data_cleaned = nifty[nifty['time_to_expiration'] < 90]
    option_data_cleaned.to_csv("options_{}.csv".format(year_to_clean), index = False)


def monthend_filter(yr):

    df_list = []
    options_data_cleaned = pd.read_csv(BASE_DIR / "options_{}.csv".format(yr), index_col=0)

    # options_date_expiry
    #options_date_expiry = pd.DataFrame()
    options_data_cleaned['EXPIRY_DT'] = pd.to_datetime(options_data_cleaned['EXPIRY_DT'], format="%Y-%m-%d")
    options_data_cleaned['EXPIRY_MONTH'] = options_data_cleaned['EXPIRY_DT'].dt.month
    options_data_cleaned['EXPIRY_DAY'] = options_data_cleaned['EXPIRY_DT'].dt.day
    options_data_cleaned['EXPIRY_YEAR'] = options_data_cleaned['EXPIRY_DT'].dt.year
    options_data_cleaned = options_data_cleaned.sort_values(by="EXPIRY_MONTH")

    # filtering the years down to 2022's expiry only
    options_data_cleaned = options_data_cleaned[options_data_cleaned['EXPIRY_DT'] < datetime.datetime.strptime(str(yr+1), "%Y")]

    max_by_months = options_data_cleaned.groupby("EXPIRY_MONTH").max()['EXPIRY_DT']
    
    options_data_monthend = options_data_cleaned[options_data_cleaned['EXPIRY_DT'].isin(max_by_months)]

    g = options_data_monthend.groupby('EXPIRY_DT')
    options_data_monthend = g.apply(lambda x: x.sort_values(by='TIMESTAMP'))

    #options_data_monthend = options_data_monthend.drop_duplicates(subset=['EXPIRY_DT', 'TIMESTAMP'])

    df_list.append(options_data_monthend)

    month_end_rows = pd.concat(df_list)
    #month_end_rows.sort_values(by='TIMESTAMP')
    month_end_rows.to_csv("month_end_rows.csv")


