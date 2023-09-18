# General libraries
from typing import List, Dict

# Libraries for directory navigation
import os

# Directories for parsing XML files and turning into dataframe
import io
import xml.etree.ElementTree as et
import pandas as pd
from collections import defaultdict

# Libraries for windowing data
from datetime import timedelta

# Libraries for saving data to SQLite database
import sqlite3
from sqlite3 import Error

# Functions for directory navigation
def read_files_from_dir(directory: str) -> List[str]:
    '''This function reads all XML files in a given directory and returns a list of XML contents as strings'''
    
    # Change to the given directory
    data_path = os.chdir(directory)
    
    print('Looking in {} directory for XML files'.format(directory))

    # Loop through given directory and collect all XML files in that directory
    file_list = []
    for filename in (os.listdir(data_path)):
        if filename.endswith('.xml'):
            full_path = os.path.join(directory, filename)
            file_list.append(full_path)
    print('Found {} XML files to process'.format(len(file_list)))
            
    # Check to make sure there are XML files to parse, if not then don't proceed
    if file_list:
            
        # Open each file as a string and append to a list
        data_list = []
        for file in file_list:
            with open(file, 'r') as f:
                data = f.read()
                data_list.append(data)
        print('Read in XML data')
    else:
        raise ValueError('No XML files found to parse..aborting')
            
    return data_list

# Function for parsing XML files and turning into dataframe

def parse_xml(files: List[str]) -> pd.DataFrame:
    '''This function takes the XML content and parses them into a dataframe'''
    
    # Initialize lists that will collect "good" XML content and "bad" XML content
    row_list = []
    bad_xml_list = []
    
    print('Beginning to parse XML content')
    
    # Loop through each XML content and grab necessary data
    for xml_file in files:
        
        xml_data = io.StringIO(u'''{}'''.format(xml_file))
        
        # Try to parse the XML content, if not then collect bad files and display later
        try:
            # Create an ElementTree object to iterate through
            etree = et.parse(xml_data) 

            # Initiate dictionary that will collect each key, value pair of data 
            row = defaultdict(list)
            
            # Loop through each element and collect attributes/texts as values
            for elem in etree.iter():
                if elem.attrib:
                    for k,v in elem.attrib.items():
                        row[k].append(v)
                elif elem.text.strip() != '':
                    row[elem.tag] = elem.text
                    
            row = dict(row)
            row_list.append(row)
            
        except:
            bad_xml_list.append(xml_file)
            
    print('Parsed all content')
    
    # Notify and display bad XML files for correction
    if bad_xml_list:
        print('Found {} file(s) with incorrect structure... Please remediate'.format(len(bad_xml_list)))
        print('-'*30)
        for bad_xml in bad_xml_list:
            print(bad_xml)
        
    # Turn the list of good rows into a dataframe
    df = pd.DataFrame(row_list)
    
    return df

# Function for changing data types
def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''This function changes data types for a dataframe'''
    
    # Identify different columns that will have data types converted
    int_cols = ['order_id']
    date_cols = ['date_time']
    float_cols = ['cost']

    # Convert columns to float
    for col in float_cols:
        df[col] = df[col].astype(float)

        # Convert columns to dates
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    # Convert columns to int
    for col in int_cols:
        df[col] = df[col].astype(int)
        
    return df

# Function for windowing dataframes
def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    '''This function takes a dataframe and window parameter and groups the data by the specified window.
    NOTE: This function uses M for minutes, H for hours, D for days, or W for weeks'''
    
    # Change columns to necessary data types
    df = format_columns(data)

    # Initialize a dictionary that will store the unit of time and dataframe associated with that time
    window_dict = {}
    
    print('Windowing the data...')

    # Parse the window string to get the window
    time_unit = window[-1].lower()
    time_quantity = int(window[:-1])

    # Get the latest event date that will be used as part of window
    latest_event = df['date_time'].max()

    # Assign the unit of time for the window
    if time_unit == 'm':
        printed_time_unit = 'minute(s)'
        time_min = latest_event - timedelta(minutes = time_quantity)
    elif time_unit == 'h':
        printed_time_unit = 'hour(s)'
        time_min = latest_event - timedelta(hours = time_quantity)
    elif time_unit == 'd':
        printed_time_unit = 'day(s)'
        time_min = latest_event - timedelta(days = time_quantity)
    elif time_unit == 'w':
        printed_time_unit = 'week(s)'
        time_min = latest_event - timedelta(weeks = time_quantity)
    else:
        raise ValueError('Time Window must be minutes, hours, days, or weeks. Please choose correct time unit')
        
    print('Window of time identified: {} {}'.format(time_quantity, printed_time_unit))

    # Filter the data down to specified window
    filtered_df = df.loc[
        (df['date_time'] <= latest_event) & (df['date_time'] >= time_min)
    ]

    # Save results to dictionary
    window_dict[window] = filtered_df
    
    print('Finished windowing the data')
    
    return window_dict

# Function for changing column names
def rename_columns(df: pd.DataFrame, rename_dict: Dict):
    '''This function renames columns for a dataframe'''
    
    df = df.rename(columns=rename_dict)
    
    return df

# Function for transforming windowed data into list of structured repair orders
def process_to_RO(data: Dict[str, pd.DataFrame]):
    '''This function takes the windowed data and transformed it into a list of structured repair orders'''

    print('Beginning to turn windowed data into list of structured dataframes')
    
    # Loop through dictionary to get window and windowed data
    for k,v in data.items():
        window = k
        window_df = v

        # Add the timeframe as a column
        window_df['time_frame'] = window

        # Flatten list columns for SQLite saving
        flatten_cols = ['name', 'quantity']
        for col in flatten_cols:
            window_df[col] = window_df[col].apply(lambda x: ', '.join(x))

        # Rename certain columns for SQLite output
        rename_cols = {
        'name': 'part_name',
        'quantity': 'part_quantity'
        }

        window_df = rename_columns(window_df, rename_cols)

        # Order the dataframe by latest orders
        window_df = window_df.sort_values(by=['date_time'], ascending=False)

        # Append the windowed data
        df_list = []

        df_list.append(window_df)
        
    print('Finished turning windowed data into list')
    
    return df_list


# Function for saving data to SQLite database
def save_to_sqlite(data: List[pd.DataFrame]):
    '''This function takes a list of dataframes and saves them to a SQLite database'''
    
    print('Number of dataframes needing to be saved to SQLite: {}'.format(len(data)))
    
    # Loop through list of dataframes and save to database
    for df in data:

        try:
    
            # Create a connection
            conn = sqlite3.connect('repair_orders.db')

            # Get which window we're using for table differentiation when saving
            window = data[0]['time_frame'].tolist()[0]

            # Save to database connection
            df.to_sql('{}_repair_orders'.format(window), conn, index=True, if_exists='replace')
            
            print('Saved {} data to repair orders database!'.format(window))
        
        except:
            print('Could not save {} dataframe...'.format(window))
    
    return 'Saving portion complete'

# ----------------------
def process_events(directory: str):
    '''Main function for collecting event updates, parsing XML files, windowing the data, 
    processing, and saving to SQLite'''
    
    # Read from directory
    data_list = read_files_from_dir(directory)
    
    # Parse XML files into dataframe
    df = parse_xml(data_list)
    
    # Window to get latest events
    window_dict = window_by_datetime(df, window='1D')
    
    # Process into structured RO format
    df_list = process_to_RO(window_dict)
    
    # Save to SQLite database

    # Commented out the save to SQLite because I was getting an error that the database was locked
    save_to_sqlite(df_list)

    print('Pipeline Complete')

if __name__ == "__main__":
    process_events(r'../data')