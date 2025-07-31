import pandas as pd
import numpy as np
import re
from datetime import datetime
import uuid
from utilities import get_connection
import os 

def clean_data(file_path):
    print("Reading CSV file...")
    df = pd.read_csv("data/raw_transactions.csv")

    print(f"Original shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(df.head())

    #Clean Column Names
    df.columns = df.columns.str.strip()

    #Fill in the Customer Names
    df['Customer Name'] = df['Customer Name'].fillna('Anonymous')  # Fill missing names
    df['Customer Name'] = df['Customer Name'].str.strip()          # Remove spaces
    df['Customer Name'] = df['Customer Name'].replace('', 'Anonymous')

    #Clean prices 
    def clean_price(price):
        if pd.isna(price):
            return np.nan
        price_str = str(price).lower().strip()
        # handle text prices
        if price_str == 'three pounds':
            return 3.0
        if price_str == 'two':
            return 2.0
        # remove £ or $ symbols
        price_str = re.sub(r'[£$]', '', price_str)
        try:
            return float(price_str)
        except ValueError:
            return np.nan

    df['Price'] = df['Price'].apply(clean_price)

    #Clean quanity
    def clean_quantity(qty):
        if pd.isna(qty):
            return np.nan
        qty_str = str(qty).strip().lower()
        if qty_str == 'two':
            return 2
        try:
            q = float(qty_str)
            return abs(int(q))  # turn negatives positive
        except:
            return np.nan

    df['Qty'] = df['Qty'].apply(clean_quantity)

    #Clean date/time
    def clean_datetime(dt):
        if pd.isna(dt):
            return np.nan
        dt_str = str(dt).strip()
        formats = ['%d/%m/%Y %H:%M', '%d-%m-%Y %H:%M', '%Y/%m/%d', '%d/%m/%Y']
        for fmt in formats:
            try:
                return pd.to_datetime(dt_str, format=fmt)
            except:
                continue
        try:
            return pd.to_datetime(dt_str, dayfirst=True)
        except:
            return np.nan

    df['Date/Time'] = df['Date/Time'].apply(clean_datetime)

    #lean Branch
    df['Branch'] = df['Branch'].fillna('Unknown').str.strip()
    df['Branch'] = df['Branch'].replace('', 'Unknown')

    #Clean Payment Type
    df['Payment Type'] = df['Payment Type'].fillna('Unknown').str.strip()
    df['Payment Type'] = df['Payment Type'].replace('', 'Unknown')

    def clean_card(card):
        if pd.isna(card):
            return None
        card_str = str(card).strip()
        if re.match(r'^\d{16}$', card_str):
            return card_str
        return None

    df['Card Number'] = df['Card Number'].apply(clean_card)

    #Clean Drink, Flavour, Milk
    for col in ['Drink', 'Flavours', 'Milk']:
        df[col] = df[col].fillna('Unknown').astype(str).str.strip()

    #Drop rows missing critical info
    before = len(df)
    df = df.dropna(subset=['Price', 'Qty', 'Date/Time'], how='any')
    after = len(df)
    print(f"Removed {before - after} bad rows")

    #Save Cleaned CSV
    output_file = "cleaned_transactions.csv"
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to: {output_file}")

    #Preview cleaned data
    print("\nSample cleaned data:")
    print(df.head(10))
    return df

#normalise data and now to add the uuids to products, branches and transaction tables
def prepare_data(df): 
    df['product_name'] = df['Drink'] + ' - ' + df['Flavours'] + ' - ' + df['Milk']
    df['product_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    #now to generate the branch ids from names with consistant uuids/
    df['branch_id'] = df['Branch'].apply(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)))

    return df

#load the data
def load_mysql(df):
    conn = get_connection()
    print(f"conn is: {conn}")
    if conn is None:
        print("Connection failed, cannot proceed.")
        return
    cursor = conn.cursor()

    #Branches
    branch = df[['branch_id', 'Branch']].drop_duplicates().values.tolist()
    cursor.executemany(
        "INSERT IGNORE INTO branches (branch_id, branch_name) VALUES (%s, %s)",
        branch
    )

    #Products
    product = df[['product_id', 'product_name', 'Price']].drop_duplicates().values.tolist()
    cursor.executemany(
        "INSERT IGNORE INTO products (product_id, product_name, product_price) VALUES (%s, %s, %s)",
        product
    )

    #Transactions
    transactions = df[['transaction_id', 'branch_id', 'Date/Time', 'Payment Type', 'Price']].values.tolist()
    cursor.executemany(
        "INSERT INTO transactions (transaction_id, branch_id, transaction_date, payment_type, total_cost) VALUES (%s, %s, %s, %s, %s)",
        transactions
    )

    #Order Items
    order_items = df[['transaction_id', 'product_id', 'Qty']].copy()
    order_items['item_id'] = [str(uuid.uuid4()) for _ in range(len(order_items))]
    orders = order_items[['item_id', 'transaction_id', 'product_id', 'Qty']].values.tolist()
    cursor.executemany(
        "INSERT INTO order_items (item_id, transaction_id, product_id, quantity) VALUES (%s, %s, %s, %s)",
        orders
    )

    conn.commit()
    cursor.close()
    conn.close()
    print("Data has been sucessfully loaded into database")


    # Call function
if __name__ == "__main__":
    df = clean_data("raw_transactions.csv")  

    df = prepare_data(df)

    load_mysql(df)

    