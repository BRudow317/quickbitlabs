import httpx
import json
import pyarrow as pa
import io

def test_get_data():
    base_url = "http://localhost:8000"
    
    with httpx.Client() as client:
        # 1. Login
        print("Logging in...")
        login_data = {
            "username": "admin",
            "password": "admin123"
        }
        resp = client.post(f"{base_url}/api/auth/login", data=login_data)
        if resp.status_code != 200:
            print(f"Login failed: {resp.status_code} {resp.text}")
            return
        
        token = resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        print("Login successful.")

        # 2. Get Data
        print("Fetching data from /api/data/...")
        # Quoting the table and column names to avoid reserved keyword conflicts (USER)
        catalog = {
            "name": "homelab",
            "source_type": "oracle",
            "entities": [
                {
                    "name": '"USER"',
                    "locator": {"plugin": "oracle"},
                    "columns": [
                        {
                            "name": '"USERNAME"', 
                            "arrow_type_id": "string",
                            "locator": {"plugin": "oracle", "entity_name": '"USER"'}
                        },
                        {
                            "name": '"EMAIL"', 
                            "arrow_type_id": "string",
                            "locator": {"plugin": "oracle", "entity_name": '"USER"'}
                        }
                    ]
                }
            ]
        }
        
        resp = client.post(f"{base_url}/api/data/", json=catalog, headers=headers)
        
        if resp.status_code == 200:
            print("Success! Received 200 OK.")
            print(f"Content-Type: {resp.headers.get('Content-Type')}")
            print(f"Content-Length: {resp.headers.get('Content-Length')}")
            
            try:
                with pa.ipc.open_stream(io.BytesIO(resp.content)) as reader:
                    table = reader.read_all()
                    print(f"Successfully read Arrow table with {table.num_rows} rows.")
                    if table.num_rows > 0:
                        print(table.to_pandas().head())
                    else:
                        print("Table is empty.")
            except Exception as e:
                print(f"Failed to parse Arrow stream: {e}")
        else:
            print(f"Request failed: {resp.status_code} {resp.text}")

if __name__ == "__main__":
    test_get_data()
