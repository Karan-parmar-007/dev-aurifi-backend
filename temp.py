import pandas as pd
import random
from datetime import datetime, timedelta
import os
# no use of this file
def generate_random_date(start_year=2025, end_year=2026):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def biased_random_choice():
    return random.choices([0, 1, 2], weights=[0.1, 0.45, 0.45])[0]

def add_columns(input_file, output_file):
    df = pd.read_excel(input_file)

    df['maturity_date'] = [generate_random_date().strftime('%Y-%m-%d') for _ in range(len(df))]
    df['overdue'] = [biased_random_choice() for _ in range(len(df))]
    df['dpd'] = [biased_random_choice() for _ in range(len(df))]
    df['restructured'] = [random.choice([True, False]) for _ in range(len(df))]
    df['rescheduled'] = [random.choice([True, False]) for _ in range(len(df))]

    df.to_excel(output_file, index=False)
    print(f"✅ Excel file with new columns saved to: {output_file}")

# Set your full input and output file paths here
input_path = r"C:\Users\Admin\Downloads\PoolFileFinal.xlsx"
output_path = r"C:\Users\Admin\Downloads\Pool_File_Updated.xlsx"

if os.path.exists(input_path):
    add_columns(input_path, output_path)
else:
    print("❌ Input file not found.")
