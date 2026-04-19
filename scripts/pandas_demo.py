import pandas as pd
import io

csv_data = """id,first_name,last_name,annual_income
1,Logan,Roy,250000
2,Kendall,Roy,150000
3,Siobhan,Roy,145000
4,Roman,Roy,130000
5,Gerri,Kellman,200000
6,Tom,Wambsgans,95000
7,Greg,Hirsch,45000
8,Marcia,Roy,180000
9,Connor,Roy,110000
10,Frank,Vernon,190000"""

customers_df = pd.read_csv(io.StringIO(csv_data))

json_data = [
    {"vin": "V001", "make": "Tesla", "model": "Model S", "miles": 12000, "owner_id": 1},
    {"vin": "V002", "make": "Lucid", "model": "Air", "miles": 500, "owner_id": 2},
    {"vin": "V003", "make": "Porsche", "model": "Taycan", "miles": 4500, "owner_id": 3},
    {"vin": "V004", "make": "Ferrari", "model": "Roma", "miles": 1200, "owner_id": 4},
    {"vin": "V005", "make": "Mercedes", "model": "S-Class", "miles": 25000, "owner_id": 5},
    {"vin": "V006", "make": "Audi", "model": "A8", "miles": 35000, "owner_id": 6},
    {"vin": "V007", "make": "Toyota", "model": "Camry", "miles": 85000, "owner_id": 7},
    {"vin": "V008", "make": "Range Rover", "model": "Vogue", "miles": 15000, "owner_id": 8},
    {"vin": "V009", "make": "Lexus", "model": "LS", "miles": 55000, "owner_id": 9},
    {"vin": "V010", "make": "BMW", "model": "750i", "miles": 22000, "owner_id": 10}
]

cars_df = pd.DataFrame(json_data)

# Format: [index, owner_id, car_vin, total_price, interest_rate, loan_length, purchase_date]
array_data = [
    [0, 1, "V001", 95000, 0.04, 60, "2024-01-15"],
    [1, 2, "V002", 85000, 0.05, 48, "2024-02-10"],
    [2, 3, "V003", 110000, 0.03, 36, "2023-11-20"],
    [3, 4, "V004", 240000, 0.06, 72, "2024-03-05"],
    [4, 5, "V005", 105000, 0.04, 60, "2023-09-12"],
    [5, 6, "V006", 75000, 0.07, 84, "2024-01-30"],
    [6, 7, "V007", 28000, 0.08, 60, "2022-05-15"],
    [7, 8, "V008", 125000, 0.04, 48, "2023-12-01"],
    [8, 9, "V009", 90000, 0.05, 60, "2024-02-28"],
    [9, 10, "V010", 115000, 0.04, 36, "2024-03-20"]
]

ledger_cols = [
    'index', 'owner_id', 'car_vin', 'total_price', 
    'interest_rate', 'loan_length_months', 'purchase_date'
]

ledger_df = pd.DataFrame(array_data, columns=ledger_cols)

# Join customers to their cars
master_df = pd.merge(customers_df, cars_df, left_on='id', right_on='owner_id')

# Join that result to the ledger
master_df = pd.merge(master_df, ledger_df, left_on=['id', 'vin'], right_on=['owner_id', 'car_vin'])

# Clean up redundant columns
master_df = master_df.drop(columns=['owner_id_x', 'owner_id_y', 'car_vin'])

print(master_df[['first_name', 'last_name', 'make', 'total_price', 'purchase_date']].head())