import numpy as np
import pandas as pd
import tarfile
import re
import os
import datetime
import io
import requests


def get_years_files(num_years):
    """
    Create list of files and list of the years we chose
    Parameters:
    -----------
    num_years: int
        The length you would like you time series to be from current time
    """
    files = os.listdir("noaa_gsod")
    files.sort()
    # take only number of years we want starting from the present
    files = files[-num_years:]
    # find all the years using regex on the digits that start each filename
    years = [int(re.findall('\d+',file)[0]) for file in files]
    
    return years, files


def get_data(directory="noaa_gsod"):
    """
    Download all data from GSOD that is currently in the Bulk Download
    
    Parameters:
    -----------
    directory: str
        The directory you want to download the tar files to. Will create it if it
        doesn't exist
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    base_url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/"
    rt = requests.get(base_url).text
    years = re.findall("(?<!\>)\d{4}", rt)
    print(years)
    for year in years:
        url = base_url+str(year)+'.tar.gz'
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(str(year)+'.tar.gz', 'wb') as f:
            #with open('noaa_data/'+str(year)+'.tar.gz', 'wb') as f:
                f.write(r.raw.read())

def process_df(df):
    """
    Clean and process the raw weather station dataframes
    Parameters:
    -----------
    df: pd.DataFrame
        Raw dataframe from station csv file to clean and process
        
    """
    #replace 9999 values with nan
    df['WDSP'] = df['WDSP'].replace(999.9, np.nan)
    df['PRCP'] = df['PRCP'].replace(99.99, np.nan)
    df[['TEMP', 'MAX', 'MIN']] =  df[['TEMP', 'MAX', 'MIN']].replace(9999.9, np.nan)
    
    #convert to datetime
    df['DATE'] = pd.to_datetime(df['DATE'])

    #keep only DATE TEMP PRCP, WDSP, MAX, MIN
    df = df[['DATE', 'TEMP','MAX', 'MIN', 'PRCP','WDSP']]

    #convert temp from farenheit to celcius
    df[['TEMP', 'MAX', 'MIN']] = df[['TEMP', 'MAX', 'MIN']].apply(lambda x: f2c(x))

    #rename columns 
    df = df.rename(columns = {'TEMP': 'Mean_Temp', 'MAX': 'Max_Temp', 'MIN': 'Min_Temp', 'PRCP': 'Mean_Precip', 'WDSP': 'Mean_WindSpeed'})
    
    return df

def aggregate_df(df_year):
    """
    Aggregates the daily weather station data to get daily mean/max/min values over all the weather stations 
    Parameters:
    -----------
    df_year: pd.DataFrame
        Combined dataframe of daily weather station data for a particular year

    """
    mean_temp = df_year.groupby('DATE')['Mean_Temp'].mean()
    max_temp = df_year.groupby('DATE')['Max_Temp'].max()
    min_temp = df_year.groupby('DATE')['Min_Temp'].min()
    mean_precip = df_year.groupby('DATE')['Mean_Precip'].mean()
    mean_windspeed = df_year.groupby('DATE')['Mean_WindSpeed'].mean()
    df_total = pd.concat([mean_temp, max_temp, min_temp, mean_precip, mean_windspeed], axis = 1)
    
    return df_total


def get_region_data(files, loc_filter, data_dir='noaa_gsod/', station_dir = 'station_id_data/'):

    """
    Get region specific weather data for a specified number of years

    Parameters:
    -----------
    files: list of strings
        List of tar.gz file names, which are grouped based on year
    loc_filter: string
        Name of the location to filter only weather stations from that location
    data_dir: string
        Directory for the raw tar.gz files
    station_dir: string
        Directory for csv station id files that group the station ids with the corresponding location 

    """
    csv_string = '_stations.csv'

    df_stations = pd.read_csv(station_dir + loc_filter + csv_string)

    #iterate over years, as files are segregated by years
    df_total = pd.DataFrame()
    for file in files:
        df_year = pd.DataFrame()
        tar = tarfile.open(data_dir + file, "r")
        #iterating over csv in tar file
        for member in tar:
            #read only stations id that are inside the stations csv
            if member.name[:-4] in df_stations['STATION_ID'].values.astype(str):
                df = pd.read_csv(io.BytesIO(tar.extractfile(member).read()), encoding = 'utf8')
                df = process_df(df)
                df_year = pd.concat([df_year, df])
        
        #after one year worth of weather data is iterated through
        #print(df_year['DATE'])
        df_year = aggregate_df(df_year)
        df_total = pd.concat([df_total, df_year])

    return df_total


def combine_region_data(loc_list, files):

    """
    Combines different region weather data into one dataframe

    Parameters:
    -----------
    loc_list: list of strings
        list of location names to obtain weather data specific to those locations

    """
    df = pd.DataFrame()
    for loc in loc_list:
        df_region = get_region_data(files, loc)
        # make a new column to indicate location tag
        df_region['Location'] = loc
        df = pd.concat([df, df_region])
    return df



#not in use
def add_meta(df):
    """
    Add metadata column to dataframe for plotly text boxes
    Parameters:
    -----------
    df: pd.DataFrame
        The final weather station dataframe to add metadata to
    """
    df['META'] = df['NAME']
    df['ELEV_LABEL'] = df['ELEVATION'].apply(lambda x: 'Elevation: '+str(x)+' m' if ~np.isnan(x) else np.nan)
    df['META'] = df[['META','ELEV_LABEL']].apply(lambda x: x.str.cat(sep='<br>'), axis=1)
    df['addmeta'] = df['TEMP'].apply(lambda x: "Temp: {} C".format(f2c(x)))
    df['META'] = df[["META", "addmeta"]].apply(lambda x: x.str.cat(sep='<br>'), axis=1)
    df = df.drop(['NAME', 'ELEV_LABEL', 'addmeta'], axis=1)
    return df



def c2f(temp):
    """
    Convert C to F
    """
    return np.round(((temp*9/5)+32),1)


def f2c(temp):
    """
    Convert F to C
    """
    return np.round(((temp-32)*5/9),1)