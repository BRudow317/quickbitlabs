That completely changes the architecture. You are absolutely right—dumping an unencrypted CSV or plain Parquet file containing millions of Salesforce records to a server's disk, even temporarily, is a massive security violation in most enterprise environments.

Since you are using PyArrow and Polars, you have two highly effective ways to solve this without getting fired. 

Here is how you handle high-throughput streaming while keeping the security team happy.

---

### Strategy 1: Parquet Modular Encryption (The "Safe Disk" Route)
You don't actually have to avoid the disk; you just have to avoid *unencrypted* disk writes. 

Apache Parquet has a feature called **Parquet Modular Encryption (PME)**. It encrypts the data at the column or file level *before* it ever touches the hard drive. PyArrow has full C++ bindings for this.

Instead of writing a plain Parquet file, you configure a `pyarrow.parquet.ParquetWriter` with an encryption configuration. The C++ backend encrypts the stream in memory, and the bytes written to disk are already ciphered.

**The catch with Polars:** Polars’ native `pl.scan_parquet()` might struggle to pass down the decryption keys to the Rust backend depending on your specific KMS setup. To bridge this, you use PyArrow to handle the cryptography and Polars to handle the math.

```python
import pyarrow.dataset as ds
import polars as pl
import pyarrow.parquet.encryption as pe

# 1. PyArrow handles the decryption of your Oracle data and SF data
# (Assuming you've set up your CryptoFactory with your KMS/Keys)
dataset = ds.dataset(
    "encrypted_salesforce_dump.parquet", 
    format="parquet", 
    # decryption_properties=my_decryption_config 
)

# 2. Pass the PyArrow dataset directly into Polars' Lazy Engine!
# Polars will lazily stream the data through PyArrow's decryption layer.
lazy_sf = pl.scan_pyarrow_dataset(dataset)
lazy_oracle = pl.scan_pyarrow_dataset(oracle_dataset)

# 3. Perform your join in memory
final_query = (
    lazy_sf
    .join(lazy_oracle, on="AccountId", how="left")
    .filter(pl.col("IsActive") == True)
)

# 4. Stream the results back out
final_df = final_query.collect(streaming=True)
```

### Strategy 2: The In-Flight "Broadcast" Join (The "No Disk" Route)
If you want to avoid writing the raw Salesforce data to disk altogether, you can process the Bulk 2.0 API stream and join it on the fly. 

This works brilliantly **IF** your Oracle dataset can fit into your server's RAM (e.g., it's a few gigabytes of reference data, like dimension tables). 

In this architecture, you load the Oracle data into memory once, and as chunks of Salesforce data stream in, you join them immediately and write the finished product out.

```python
import polars as pl

def process_sf_bulk_stream(sf_api_generator, encrypted_oracle_path):
    # 1. Load the ENTIRE Oracle reference dataset into RAM once.
    # (Assuming you decrypt it into memory here)
    oracle_df = pl.read_parquet(encrypted_oracle_path) 
    
    # 2. Iterate through your Salesforce Bulk 2.0 chunks
    for raw_sf_chunk in sf_api_generator:
        
        # A. Convert the API chunk to a Polars DataFrame (in memory)
        sf_chunk_df = pl.DataFrame(raw_sf_chunk)
        
        # B. Join it against the Oracle data immediately (in memory)
        joined_chunk = sf_chunk_df.join(
            oracle_df, 
            on="AccountId", 
            how="inner"
        )
        
        # C. Write the enriched chunk directly to your final destination
        # (e.g., pushing to a database, or appending to an encrypted final Parquet file)
        write_to_secure_destination(joined_chunk)
        
        # Memory is automatically freed when the loop restarts
```

### Which one should you choose?

* **Choose Strategy 1 (PME + PyArrow Dataset)** if your Oracle data is **too massive** to fit in RAM. You will stream the Salesforce API to an encrypted Parquet file on disk first, then let Polars stream-join the two encrypted files together.
* **Choose Strategy 2 (In-Flight)** if your Oracle data is relatively small (under 10GB). This is the fastest and most secure method because the raw Salesforce data literally never touches a hard drive; it lives in RAM for a few milliseconds, gets joined, and is immediately pushed to its final secure destination. 

How large is the Oracle dataset you are trying to join against? Is it something you can reasonably hold in RAM, or does it require disk-spilling?