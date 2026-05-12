import polars as pl
from pathlib import Path
import random
import string
from datetime import date, datetime, timedelta
import hashlib
import json

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================
MOCK_DATA_LIMIT = 5000         # Number of customers to process
SKIP_PROBABILITY = 0.2         # 20% chance to skip (and reassign to previous)
ZIP_PLUS_4_PROBABILITY = 0.5   # 50% chance for 9-digit zip code
RANDOM_ADDRESS_ALPHA_PROB = 0.8 # Chance for a letter suffix in address

# Primary Key Start Sequences
START_ID_DEMOGRAPHIC = 1000
START_ID_PHONE       = 2000
START_ID_ADDRESS     = 3000
START_ID_ACCOUNT     = 4000
START_ID_EMPLOYMENT  = 5000

# Lookup Category tiered numbering
LOOKUP_CONFIGS = [
    ("GENDER",         ["MALE", "FEMALE", "UNKNOWN"],           1),
    ("MAR_STATUS",     ["MARRIED", "SINGLE", "UNKNOWN"],        10),
    ("NAME_PREFIX",    ["MR", "MS", "DR", "PROF"],              21),
    ("NAME_SUFFIX",    ["JR", "SR", "II", "III"],               31),
    ("ADDRESS_TYPE",   ["HOME", "WORK", "MAILING"],             41),
    ("PHONE_TYPE",     ["MAIN", "MOBILE", "FAX"],               51),
    ("DELETED_STATUS", ["ACTIVE", "INACTIVE", "PENDING"],       61),
    ("ACCOUNT_STATUS", ["ACTIVE", "SUSPENDED", "CLOSED"],       71),
    ("ACCOUNT_TYPE",   [],                                      100000) # Industries dynamic
]

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "MD", "MA", 
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", 
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
    "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

COUNTY_SUFFIXES = ["County", "Parish", "Borough"]
STREET_TYPES = ["St", "Ave", "Blvd", "Ln", "Dr", "Ct", "Way"]

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def random_address_line():
    num = random.randint(100, 9999)
    suffix = random.choice(string.ascii_uppercase) if random.random() > RANDOM_ADDRESS_ALPHA_PROB else ""
    name = "".join(random.choices(string.ascii_uppercase, k=random.randint(5, 10)))
    street_type = random.choice(STREET_TYPES)
    return f"{num}{suffix} {name} {street_type}"

def random_county():
    name = "".join(random.choices(string.ascii_uppercase, k=random.randint(6, 12)))
    suffix = random.choice(COUNTY_SUFFIXES)
    return f"{name} {suffix}"

def random_zip_code():
    zip5 = "".join(random.choices(string.digits, k=5))
    if random.random() > ZIP_PLUS_4_PROBABILITY:
        zip4 = "".join(random.choices(string.digits, k=4))
        return f"{zip5}-{zip4}"
    return zip5

def generate_random_birthdate():
    today = date.today()
    start_date = today - timedelta(days=365*70)
    end_date = today - timedelta(days=365*19)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, max(0, days_between))
    return start_date + timedelta(days=random_days)

def generate_ssn(seed_str):
    h = hashlib.md5(seed_str.encode()).hexdigest()
    return f"{int(h[:3], 16)%900+100:03}{int(h[3:5], 16)%90+10:02}{int(h[5:9], 16)%9000+1000:04}"

def write_pretty_json(df, file_path, column_order):
    for col in column_order:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    df = df.select(column_order)
    data = df.to_dicts()
    for row in data:
        for key, value in row.items():
            if isinstance(value, (date, datetime)):
                row[key] = value.isoformat()
            elif value is None:
                row[key] = None
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

# ==============================================================================
# MAIN GENERATION LOGIC
# ==============================================================================

def generate_full_mock_data(base_path: str|Path) -> None:
    output_dir = Path(base_path)
    
    # 1. Load Base Data
    print(f"Loading base templates (Limit: {MOCK_DATA_LIMIT})...")
    cust_df = pl.read_csv(output_dir / "customers-100000.csv").head(MOCK_DATA_LIMIT)
    org_df = (pl.read_csv(output_dir / "org.csv")
              .unique(subset=["Organization Id"], keep="first")
              .unique(subset=["Name"], keep="first"))
    
    cust_df = cust_df.with_columns([
        pl.arange(START_ID_DEMOGRAPHIC, START_ID_DEMOGRAPHIC + cust_df.height).alias("demographic_id"),
        pl.col("Customer Id").map_elements(lambda x: hashlib.md5(x.encode()).hexdigest()[:16], return_dtype=pl.String).alias("Customer Id")
    ])
    cust_list = cust_df.to_dicts()
    
    # 2. Update MQ_LOOKUP
    print("Updating Lookup data...")
    industries = org_df["Industry"].unique().to_list()
    
    lookup_data = []
    category_codes_map = {} 
    
    for category, labels, start_code in LOOKUP_CONFIGS:
        if category == "ACCOUNT_TYPE":
            labels = industries
            
        category_codes_map[category] = []
        for i, label in enumerate(labels):
            code = start_code + i
            lookup_data.append({
                "lookup_id": code,
                "category": category,
                "lookup_desc": label,
                "created_at": None, "created_by": None, "updated_at": None, "updated_by": None
            })
            category_codes_map[category].append(code)

    lookup_cols = ["lookup_id", "category", "lookup_desc", "created_at", "created_by", "updated_at", "updated_by"]
    write_pretty_json(pl.DataFrame(lookup_data), output_dir / "mq_lookup.json", lookup_cols)
    
    # 3. Generate EMPLOYEE
    print("Generating Employee data...")
    employee_df = cust_df.select([pl.col("Customer Id").alias("employee_id")])
    employee_df = employee_df.with_columns([
        pl.col("employee_id").map_elements(lambda x: generate_ssn(x), return_dtype=pl.String).alias("ssn"),
        pl.lit(None).alias("created_at"), pl.lit(None).alias("created_by"), 
        pl.lit(None).alias("updated_at"), pl.lit(None).alias("updated_by")
    ]).sort("employee_id")
    write_pretty_json(employee_df, output_dir / "employee.json", ["employee_id", "ssn", "created_at", "created_by", "updated_at", "updated_by"])
    
    # 4. Generate DEMOGRAPHIC
    print("Generating Demographic data...")
    demogr_df = cust_df.select([
        pl.col("demographic_id"),
        pl.col("Customer Id").alias("employee_id"),
        pl.col("First Name").alias("first_name"),
        pl.col("Last Name").alias("last_name"),
        pl.col("Email").alias("email_addr")
    ]).with_columns([
        pl.lit(None).alias("middle_name"),
        pl.Series([random.choice(category_codes_map["NAME_PREFIX"]) for _ in range(cust_df.height)]).alias("prefix_id"),
        pl.Series([random.choice(category_codes_map["NAME_SUFFIX"]) for _ in range(cust_df.height)]).alias("suffix_id"),
        pl.lit(None).alias("dt_of_death"),
        pl.lit(None).alias("created_at"), pl.lit(None).alias("created_by"), 
        pl.lit(None).alias("updated_at"), pl.lit(None).alias("updated_by"),
        pl.Series([generate_random_birthdate() for _ in range(cust_df.height)]).alias("birthdate"),
        pl.Series([random.choice(category_codes_map["GENDER"]) for _ in range(cust_df.height)]).alias("gender_id"),
        pl.Series([random.choice(category_codes_map["MAR_STATUS"]) for _ in range(cust_df.height)]).alias("mar_status_id"),
        pl.lit(category_codes_map["DELETED_STATUS"][0]).alias("deleted_status")
    ]).sort("demographic_id")
    
    demographic_cols = [
        "demographic_id", "employee_id", "first_name", "middle_name", "last_name",
        "prefix_id", "suffix_id", "birthdate", "dt_of_death", "gender_id",
        "mar_status_id", "email_addr", "deleted_status", "created_at", "created_by", "updated_at", "updated_by"
    ]
    write_pretty_json(demogr_df, output_dir / "demographic.json", demographic_cols)
    
    # 5 & 6. Generate PHONE and ADDRESS with INDEPENDENT Reassignment Logic
    print("Generating Phone and Address data with independent reassignment...")
    phones_final = []
    addresses_final = []
    
    assigned_phone_types = {row["demographic_id"]: [] for row in cust_list}
    assigned_addr_types = {row["demographic_id"]: [] for row in cust_list}
    
    current_phone_id = START_ID_PHONE
    current_addr_id = START_ID_ADDRESS
    
    for i, person in enumerate(cust_list):
        demo_id = person["demographic_id"]
        
        # --- Handle Phone ---
        p_skip = random.random() < SKIP_PROBABILITY
        p_target_id = demo_id
        if p_skip and i > 0:
            p_target_id = cust_list[i-1]["demographic_id"]
            
        if not (p_skip and i == 0):
            p_types = category_codes_map["PHONE_TYPE"]
            available_p_types = [t for t in p_types if t not in assigned_phone_types[p_target_id]]
            if available_p_types:
                p_type = random.choice(available_p_types)
                assigned_phone_types[p_target_id].append(p_type)
                phones_final.append({
                    "phone_id": current_phone_id,
                    "demographic_id": p_target_id,
                    "phone": person["Phone 1"],
                    "phone_ext": None,
                    "phone_type_id": p_type,
                    "created_at": None, "created_by": None, "updated_at": None, "updated_by": None
                })
                current_phone_id += 1
            
        # --- Handle Address ---
        a_skip = random.random() < SKIP_PROBABILITY
        a_target_id = demo_id
        if a_skip and i > 0:
            a_target_id = cust_list[i-1]["demographic_id"]
            
        if not (a_skip and i == 0):
            a_types = category_codes_map["ADDRESS_TYPE"]
            available_a_types = [t for t in a_types if t not in assigned_addr_types[a_target_id]]
            if available_a_types:
                a_type = random.choice(available_a_types)
                assigned_addr_types[a_target_id].append(a_type)
                addresses_final.append({
                    "address_id": current_addr_id,
                    "demographic_id": a_target_id,
                    "address1": random_address_line(),
                    "address2": None, "address3": None, "address4": None,
                    "city": person["City"],
                    "county": random_county(),
                    "state": random.choice(US_STATES),
                    "postal": random_zip_code(),
                    "country": "USA",
                    "address_type_id": a_type,
                    "created_at": None, "created_by": None, "updated_at": None, "updated_by": None
                })
                current_addr_id += 1

    phone_cols = ["phone_id", "demographic_id", "phone", "phone_ext", "phone_type_id", "created_at", "created_by", "updated_at", "updated_by"]
    write_pretty_json(pl.DataFrame(phones_final).sort("demographic_id"), output_dir / "phone.json", phone_cols)
    
    address_cols = [
        "address_id", "demographic_id", "address1", "address2", "address3", "address4",
        "city", "county", "state", "postal", "country", "address_type_id",
        "created_at", "created_by", "updated_at", "updated_by"
    ]
    write_pretty_json(pl.DataFrame(addresses_final).sort("demographic_id"), output_dir / "address.json", address_cols)
    
    # 7. Generate ACCOUNT
    print("Generating Account data...")
    industry_id_map = {label: (100000 + i) for i, label in enumerate(industries)}
    account_df = org_df.select([
        pl.col("Organization Id").alias("account_code"),
        pl.col("Name").alias("account_name"),
        pl.col("Industry").alias("industry_label")
    ]).with_columns([
        pl.arange(START_ID_ACCOUNT, START_ID_ACCOUNT + org_df.height).alias("account_id"),
        pl.col("industry_label").replace_strict(industry_id_map, default=None).alias("account_type_id"),
        pl.Series([random.choice(category_codes_map["ACCOUNT_STATUS"]) for _ in range(org_df.height)]).alias("account_status_id"),
        pl.lit(None).alias("created_at"), pl.lit(None).alias("created_by"), 
        pl.lit(None).alias("updated_at"), pl.lit(None).alias("updated_by")
    ]).drop("industry_label").sort("account_code")
    write_pretty_json(account_df, output_dir / "account.json", ["account_id", "account_code", "account_name", "account_status_id", "account_type_id", "created_at", "created_by", "updated_at", "updated_by"])

    # 8. Generate EMPLOYMENT
    print("Generating Employment data...")
    account_codes = account_df["account_code"].to_list()
    employment_df = demogr_df.select(["demographic_id", "birthdate"]).with_columns([
        pl.arange(START_ID_EMPLOYMENT, START_ID_EMPLOYMENT + demogr_df.height).alias("employment_id"),
        pl.Series([random.choice(account_codes) for _ in range(demogr_df.height)]).alias("account_code"),
        pl.lit(None).alias("termination_dt"),
        pl.lit(None).alias("created_at"), pl.lit(None).alias("created_by"), 
        pl.lit(None).alias("updated_at"), pl.lit(None).alias("updated_by")
    ])
    employment_df = employment_df.with_columns([
        pl.col("birthdate").map_elements(
            lambda b: min(date.fromisoformat(b.isoformat()) + timedelta(days=365*18 + random.randint(0, 365*10)), date.today()),
            return_dtype=pl.Date
        ).alias("hire_dt")
    ]).drop("birthdate").sort("demographic_id")
    employment_cols = ["employment_id", "demographic_id", "account_code", "hire_dt", "termination_dt", "created_at", "created_by", "updated_at", "updated_by"]
    write_pretty_json(employment_df, output_dir / "employment.json", employment_cols)

    print("Mock data generation complete! Output: Randomized Relational JSON")

if __name__ == "__main__":
    generate_full_mock_data(r"Q:\quickbitlabs\build\mock_data")
