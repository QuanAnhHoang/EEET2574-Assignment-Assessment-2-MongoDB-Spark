import csv
import os
import pymongo

# Connect to MongoDB (Make sure MongoDB is running on your machine)
client = pymongo.MongoClient('mongodb+srv://admin:vwcfVfpNtqANLbAQ@cluster0.zuy7zeb.mongodb.net/')
db = client['testdb']

# Select or create a collection within the database
Gas = db['Gas']

# Path to the directory containing CSV files
csv_directory = 'EEET2574_Assignment2_data/Gas'

# Iterate through CSV files in the directory
for csv_file_name in os.listdir(csv_directory):
    if csv_file_name.endswith('.csv'):
        csv_file_path = os.path.join(csv_directory, csv_file_name)

        # Extract the year from the filename
        year = int(csv_file_name.split('_')[-1].split('.')[0])

        # Read data from CSV file
        data_to_insert = []
        with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                # Add the "year" field to each document
                row['year'] = year
                data_to_insert.append(row)

        # Insert data into MongoDB
        result = Gas.insert_many(data_to_insert)
        print(f"Inserted documents for year {year}. IDs: {result.inserted_ids}")

# Close the MongoDB connection
client.close()