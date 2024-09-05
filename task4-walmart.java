import pandas as pd
import sqlite3

# Path to the SQLite database
DATABASE_PATH = 'shipping_data.db'

# Paths to the spreadsheet files
SPREADSHEET_0_PATH = 'spreadsheet_0.xlsx'
SPREADSHEET_1_PATH = 'spreadsheet_1.xlsx'
SPREADSHEET_2_PATH = 'spreadsheet_2.xlsx'

# Connect to the SQLite database
connection = sqlite3.connect(DATABASE_PATH)
cursor = connection.cursor()

# Function to insert data from spreadsheet 0 directly into the database
def insert_data_from_spreadsheet_0():
    df = pd.read_excel(SPREADSHEET_0_PATH)
    df.to_sql('shipment_data', connection, if_exists='append', index=False)
    print("Data from spreadsheet 0 inserted successfully.")

# Function to insert data from spreadsheets 1 and 2
def insert_data_from_spreadsheets_1_and_2():
    # Read data from both spreadsheets
    df1 = pd.read_excel(SPREADSHEET_1_PATH)
    df2 = pd.read_excel(SPREADSHEET_2_PATH)

    # Merge data based on 'shipment_identifier'
    merged_df = pd.merge(df1, df2, on='shipment_identifier', how='left')

    # Create a new DataFrame to insert into the database
    final_data = []
    for _, row in merged_df.iterrows():
        shipment_id = row['shipment_identifier']
        products = row['product_name'].split(';')  # Assuming products are separated by semicolons
        quantities = [int(q) for q in row['quantity'].split(';')]  # Assuming quantities are separated by semicolons
        origin = row['origin']
        destination = row['destination']
        for product, quantity in zip(products, quantities):
            final_data.append({
                'shipment_id': shipment_id,
                'product_name': product.strip(),
                'quantity': quantity,
                'origin': origin,
                'destination': destination
            })
    
    # Convert the list to a DataFrame
    final_df = pd.DataFrame(final_data)

    # Insert the data into the database
    final_df.to_sql('shipment_data', connection, if_exists='append', index=False)
    print("Data from spreadsheets 1 and 2 inserted successfully.")

# Main function to run the ETL process
def main():
    insert_data_from_spreadsheet_0()
    insert_data_from_spreadsheets_1_and_2()
    connection.commit()
    connection.close()
    print("All data inserted successfully and connection closed.")

if __name__ == "__main__":
    main()
