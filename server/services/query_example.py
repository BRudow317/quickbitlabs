import polars as pl
from server.plugins.PluginRegistry import get_plugin
from server.plugins.PluginModels import (
Catalog, Entity, Column, 
FilterGroup, Filter, Sort, Join
)



# 1. Instantiate the Plugin Facade via the Factory
oracle = get_plugin('oracle')

# 2. DISCOVERY PHASE: We don't know the schema, so we ask the plugin to map it.
master_catalog = Catalog(name="sf", properties={"schema": "sf"})
# We pass an empty catalog shell targeting the "sf" schema
response = oracle.get_catalog(master_catalog)

if not response.ok: raise RuntimeError(f"Failed to discover schema: {response.message}")
if not response.data or not response.data.entities: raise RuntimeError("Discovery returned empty schema.")

# The Master Suitcase is now fully hydrated with all tables, fields, and Arrow types.
master_catalog = response.data


# =====================================================================
# TASK 1: Complex Analytical Extraction (The Pushdown + Polars Bridge)
# Goal: Lowest score users on cases with contacts on accounts > 100 cases since 2026.
# =====================================================================

# We create an Execution Catalog by pulling just the entities we need 
# from our Master Catalog to avoid querying the entire database.
user = master_catalog.entity_map['user']
case_score = master_catalog.entity_map['case_score__c']
case = master_catalog.entity_map['case']
contact = master_catalog.entity_map['contact']
account = master_catalog.entity_map['account']

exec_catalog = Catalog (
    name=master_catalog.name,
    properties=master_catalog.properties,
    entities=[ user, case_score, case, contact, account ]
)

# Apply Cross-Entity Logic (The Joins)
exec_catalog.joins = [
    Join(left_entity=exec_catalog.entity_map['user'], 
            left_field=exec_catalog.entity_map['user'].field_map['id'], 
            right_entity=exec_catalog.entity_map['case_score__c'], 
            right_field=exec_catalog.entity_map['case_score__c'].field_map["user_id"]
            ),
    Join(left_entity=exec_catalog.entity_map['case_score__c'], 
            left_field=exec_catalog.entity_map['case_score__c'].field_map["case_id"], 
            right_entity=exec_catalog.entity_map['case'], 
            right_field=exec_catalog.entity_map['case'].field_map["id"]
            ),
    Join(left_entity=exec_catalog.entity_map['case'], 
            left_field=exec_catalog.entity_map['case'].field_map["contact_id"], 
            right_entity=exec_catalog.entity_map['contact'], 
            right_field=exec_catalog.entity_map['contact'].field_map["id"]
            ),
    Join(left_entity=exec_catalog.entity_map['contact'], 
            left_field=exec_catalog.entity_map['contact'].field_map["account_id"], 
            right_entity=exec_catalog.entity_map['account'], 
            right_field=exec_catalog.entity_map['account'].field_map["id"]
            )
]

# Apply Scoped Level-2 Filter: Cases since 2026 began
exec_catalog.entity_map['case'].filter_group = FilterGroup(
    condition="AND",
    filters=[Filter(independent="created_date", operator=">=", dependent="2026-01-01")]
)

# Execute extraction! Oracle pushes the Joins and the Date filter down into the DB.
stream_response = oracle.get_data(exec_catalog)
df_master = pl.from_arrow(stream_response.data)

# Polars handles the complex aggregation (Accounts with > 100 cases) in memory
# (Since aggregation isn't in our AST yet, Polars is the perfect engine for this step).
valid_accounts = (
    df_master.group_by("account.id")
    .agg(pl.count("case.id").alias("case_count"))
    .filter(pl.col("case_count") > 100)
)

# Filter the master dataframe to only include those valid accounts, 
# then sort to find the lowest case scores.
df_target_users = (
    df_master.join(valid_accounts, on="account.id", how="inner")
    .sort("case_score__c.score", descending=False)
)

# We now have our answer for Task 1!
print(df_target_users.head())

# =====================================================================
# TASK 2: DDL Schema Modification
# Goal: Create "good_reps_only" column on Accounts with >= 10 contacts.
# =====================================================================

account_entity = master_catalog.entity_map['account']

# 1. Define the new Field Model
new_col = Column(
    name="good_reps_only", 
    arrow_type_id="bool", 
    nullable=True, 
    source_description="Accounts with 10+ contacts"
)

# 2. Execute DDL via the Protocol (Oracle runs ALTER TABLE ADD COLUMN)
oracle.create_field(catalog=master_catalog, entity=account_entity, field=new_col)

# 3. Update the data using Polars for logic and Oracle for DML
# Find accounts with >= 10 contacts
df_contacts = df_master.select(["account.id", "contact.id"]).unique()
accounts_to_flag = (
    df_contacts.group_by("account.id")
    .agg(pl.count("contact.id").alias("contact_count"))
    .filter(pl.col("contact_count") >= 10)
    .select(
        pl.col("account.id").alias("id"), 
        pl.lit(True).alias("good_reps_only") # Mutate value
    )
)

# Execute DML via Protocol (Oracle updates the newly created column)
update_stream = accounts_to_flag.to_arrow().to_reader() # Convert DataFrame -> RecordBatchReader
oracle.upsert_data(master_catalog, account_entity, data=update_stream)

# =====================================================================
# TASK 3: DML Write-Back (The Terminations)
# Goal: Upsert the status of the 5 users with the absolute lowest case scores.
# =====================================================================

# Build a highly specific Execution Catalog for this extraction
term_catalog = Catalog(
    name=master_catalog.name,
    entities=[master_catalog.entity_map['user'], master_catalog.entity_map['case_score__c']],
    joins=[Join(left_entity="user", left_field="id", right_entity="case_score__c", right_field="user_id")]
)

# Apply Sorting and Limits directly to the entities!
term_catalog.entity_map['case_score__c'].sorts = [Sort(field="score", direction="ASC")]
term_catalog.entity_map['user'].limit = 5

# Extract the worst 5 users
worst_users_response = oracle.get_data(term_catalog)
df_worst_users = pl.from_arrow(worst_users_response.data)

# Mutate the dataframe in memory to set status
df_terminated = df_worst_users.with_columns(pl.lit("terminated").alias("status"))

# Push the mutation back to Oracle!
term_stream = df_terminated.to_arrow().to_reader()
oracle.upsert_data(master_catalog, master_catalog.entity_map['user'], data=term_stream)

