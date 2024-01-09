#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import requests
import warnings
import yfinance as yf
import time
from datetime import datetime
import sqlite3
import threading
import os

# Create a database for option data

database_path = os.path.expanduser('~/option_database/options.db')

# Connect to the SQLite database. If it doesn't exist, it will be created.
conn = sqlite3.connect(database_path)

# Create a cursor object using the cursor method
cursor = conn.cursor()
#'contractSymbol', 'lastTradeDate', 'strike', 'lastPrice', 'bid', 'ask',
    #    'change', 'percentChange', 'volume', 'openInterest',
    #    'impliedVolatility', 'inTheMoney', 'contractSize', 'currency'
# SQL statement to create a table for call options
create_table_calls = """
CREATE TABLE IF NOT EXISTS calls (
    contractSymbol TEXT,
    timestamp DATETIME,
    lastTradeDate TEXT,
    strike REAL,
    lastPrice REAL,
    bid REAL,
    ask REAL,
    change REAL,
    percentChange REAL,
    volume INTEGER,
    openInterest INTEGER,
    impliedVolatility REAL,
    inTheMoney TEXT,
    contractSize TEXT,
    currency TEXT,
    expiration_date TEXT,
    PRIMARY KEY (contractSymbol, timestamp)
);
"""

# SQL statement to create a table for put options
create_table_puts = """
CREATE TABLE IF NOT EXISTS puts (
    contractSymbol TEXT,
    timestamp DATETIME,
    lastTradeDate TEXT,
    strike REAL,
    lastPrice REAL,
    bid REAL,
    ask REAL,
    change REAL,
    percentChange REAL,
    volume INTEGER,
    openInterest INTEGER,
    impliedVolatility REAL,
    inTheMoney TEXT,
    contractSize TEXT,
    currency TEXT,
    expiration_date TEXT,
    PRIMARY KEY (contractSymbol, timestamp)
);
"""

# Execute the SQL statements to create the tables
cursor.execute(create_table_calls)
cursor.execute(create_table_puts)

# Commit the changes
conn.commit()

# Close the database connection
conn.close()



# Fetch data from Yfiance

# In[ ]:


# stock symbols
stock_symbols = ['TSLA', 'AAPL', 'GOOGL', 'AMZN', 'PLTR', 'AMD', 'NVDA']

# Function to fetch options data
def fetch_options_data(symbol, expiration_date):
    ticker = yf.Ticker(symbol)
    options_chain = ticker.option_chain(expiration_date)
    return options_chain.calls, options_chain.puts

# Function to insert data into the database
def insert_data_to_db(db_connection, table_name, data_frame, expiration_date):
    data_frame['timestamp'] = datetime.now() # Add a timestamp column to the dataframe with the current time
    data_frame['expiration_date'] = expiration_date
    data_frame.to_sql(table_name, db_connection, if_exists='append', index=False)

# Function to fetch and store data for a specific symbol and expiration date
def handle_data_for_symbol_and_date(symbol, expiration_date, interval, stop_event):
    while not stop_event.is_set():
        conn = sqlite3.connect(database_path)
        try:
            # Fetch the options data
            calls, puts = fetch_options_data(symbol, expiration_date)
            
            # Insert the data into the database
            insert_data_to_db(conn, 'calls', calls, expiration_date)
            insert_data_to_db(conn, 'puts', puts, expiration_date)
        except Exception as e:
            print("An error occurred:", e)
        finally:
            conn.close()
        
        # Wait for the specified interval (e.g., 1 hour)
        time.sleep(interval)

# Main function to initiate threads
def main():
    update_interval = 3600  # Update every 1 hour
    threads = []
    stop_events = []

    for symbol in stock_symbols:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options

        for expiration_date in expirations:
            stop_event = threading.Event()
            thread = threading.Thread(target=handle_data_for_symbol_and_date, 
                                      args=(symbol, expiration_date, update_interval, stop_event))
            thread.start()
            threads.append(thread)
            stop_events.append(stop_event)

    # Here you can implement the logic to stop the threads by setting the stop_events

# Run the script
if __name__ == "__main__":
    main()


