import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
import mysql.connector
from mysql.connector import Error
from db_config import db_config  # Import the db_config dictionary

# Path to the CSV file
csv_file_path = r'your-file-path'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Check if the 'url' column exists
if 'link' not in df.columns:
    raise KeyError("The column 'url' does not exist in the CSV file.")

# Extract the URLs from the 'url' column and store them in a list
target_urls = df['link'].tolist()

# Initialize the list to store profile data
profile_list = []

# Initialize the WebDriver
driver = webdriver.Firefox()

for target_url in target_urls:
    # Initialize the dictionary to store profile data
    profile_data = {}

    # Open the target URL
    driver.get(target_url)
    time.sleep(2)

    # Get the page source
    resp = driver.page_source

    # Parse the HTML content
    soup = BeautifulSoup(resp, 'html.parser')

    # Extract profile information
    try:
        profile_data["Profile_Name"] = soup.find("div", {"class": "r-1vr29t4"}).text
    except Exception as e:
        profile_data["Profile_Name"] = None
        print(f"Error extracting Profile_Name: {e}")

    try:
        profile_data["Profile_Handle"] = soup.find("div", {"class": "r-1wvb978"}).text
    except Exception as e:
        profile_data["Profile_Handle"] = None
        print(f"Error extracting Profile_Handle: {e}")

    try:
        profile_data["Bio"] = soup.find("div", {"data-testid": "UserDescription"}).text
    except Exception as e:
        profile_data["Bio"] = None
        print(f"Error extracting Bio: {e}")

    try:
        profile_data["Location"] = soup.find("span", {"data-testid": "UserLocation"}).text
    except Exception as e:
        profile_data["Location"] = None
        print(f"Error extracting Location: {e}")

    profile_header = soup.find("div", {"data-testid": "UserProfileHeader_Items"})

    try:
        profile_data["Website"] = profile_header.find('a').get('href')
    except Exception as e:
        profile_data["Website"] = None
        print(f"Error extracting Website: {e}")

    try:
        profile_data["Following_Count"] = soup.find_all("a", {"class": "r-rjixqe"})[0].text
    except Exception as e:
        profile_data["Following_Count"] = None
        print(f"Error extracting Following_Count: {e}")

    try:
        profile_data["Followers_Count"] = soup.find_all("a", {"class": "r-rjixqe"})[1].text
    except Exception as e:
        profile_data["Followers_Count"] = None
        print(f"Error extracting Followers_Count: {e}")

    # Append the profile data to the list
    profile_list.append(profile_data)

# Close the WebDriver
driver.quit()

# Convert the list of dictionaries to a Pandas DataFrame
df_profiles = pd.DataFrame(profile_list)

# Connect to the MySQL server
conn = mysql.connector.connect(
    user=db_config['user'],
    password=db_config['password'],
    host=db_config['host']
)
cursor = conn.cursor()

# Create the database if it does not exist
database_name = db_config['database']
create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
cursor.execute(create_db_query)

# Use the database
use_db_query = f"USE {database_name}"
cursor.execute(use_db_query)

# Create the table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS twitter_profiles (
    Profile_Name VARCHAR(255),
    Profile_Handle VARCHAR(255),
    Bio TEXT,
    Location VARCHAR(255),
    Website VARCHAR(255),
    Following_Count VARCHAR(255),
    Followers_Count VARCHAR(255)
)
"""
cursor.execute(create_table_query)

# Insert data into the table
insert_query = """
INSERT INTO twitter_profiles (Profile_Name, Profile_Handle, Bio, Location, Website, Following_Count, Followers_Count)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

for index, row in df_profiles.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Data successfully inserted into the MySQL database.")