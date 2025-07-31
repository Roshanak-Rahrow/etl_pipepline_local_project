import pandas as pd
import random
from datetime import datetime, timedelta
import numpy as np

# Setup
branches = ["Ealing", "Croydon", "Nottinghill", "Brixton", "Camden"]
drinks = ["Latte", "Cappuccino", "Espresso", "Mocha", "Americano", "Flat White", "Matcha Latte", "Hot Chocolate", "Tea"]
flavours = ["No Flavour", "Vanilla", "Caramel", "hazelnut", "Oolong", "Green", "English Breakfast", "Peppermint"]
milks = ["No milk", "1% Milk", "Semi-Skimmed Milk", "Oa", "Coconut", "Soya"]
payment_types = ["Card", "Cash", "Mobile"]
bad_branches = ["Soho", "London Bridge", ""]  # For incorrect branch examples
bad_prices = ["$3.50", "Three Pounds", "£two", "3.50", None]
bad_qty = ["two", "", None, "3.0", -1]
bad_dates = ["33/13/2024 10:00", "18-08-2024 12:45", "2024/08/18", None]
bad_card_numbers = ["1234abcd5678", "notacard", "", None]

# Utility to generate a random valid date/time string
def random_date():
    start = datetime(2024, 1, 1, 7, 0)
    end = datetime(2024, 12, 31, 18, 0)
    delta = end - start
    random_minute = random.randint(0, int(delta.total_seconds() / 60))
    return (start + timedelta(minutes=random_minute)).strftime("%d/%m/%Y %H:%M")


# Function to generate one row of transactional data with extended attributes
def generate_row_extended():
    row = {}
    row['Customer Name'] = random.choice(["Dave", "Sarah", "Ali", "Jessica", "Tom", "Priya", "Daniel", "Emily", "Chris", ""]) if random.random() > 0.05 else None
    
    drink = random.choice(drinks)
    row['Drink'] = drink

    #define flavours 
    tea_flavours = ["Oolong", "Green", "English Breakfast", "Peppermint"]
    coffee_flavours = ["No Flavour", "Vanilla", "Caramel", "Hazelnut"]

    #assing flavours
    if drink == "Tea":
        row['Flavours'] = random.choice(tea_flavours)
    elif drink in ["Latte", "Cappuccino", "Espresso", "Mocha", "Americano", "Flat White", "Matcha Latte", "Hot Chocolate"]:
        row['Flavours'] = random.choice(coffee_flavours)
    else:
        row['Flavours'] = None
  
    #define milk 
    if drink == ["Oolong", "Green","Peppermint"]:
        row['Milk'] = "No milk" 
    else: 
        row["Milk"] = random.choice(milks)

    # Randomly insert bad or valid quantity
    row['Qty'] = random.choice(bad_qty) if random.random() < 0.1 else random.randint(1, 4)
    # Randomly insert bad or valid price
    row['Price'] = random.choice(bad_prices) if random.random() < 0.1 else f"£{round(random.uniform(6.0, 26), 2)}"
    # Randomly use valid or invalid branch
    row['Branch'] = random.choice(bad_branches) if random.random() < 0.1 else random.choice(branches)
    # Randomly set payment type to null or invalid
    row['Payment Type'] = random.choice(payment_types + ["", None]) if random.random() < 0.1 else random.choice(payment_types)
    # Randomly use valid or invalid card number
    row['Card Number'] = random.choice(bad_card_numbers) if random.random() < 0.1 else str(random.randint(4000000000000000, 4999999999999999))
    # Randomly insert invalid date format
    row['Date/Time'] = random.choice(bad_dates) if random.random() < 0.1 else random_date()
    return row

# Generate 100 rows of extended data
extended_data = [generate_row_extended() for _ in range(100)]
df_extended = pd.DataFrame(extended_data)

# Save to CSV
extended_file_path = "./data/raw_transations.csv"
df_extended.to_csv(extended_file_path, index=False)

extended_file_path