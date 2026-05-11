from __future__ import annotations
from pathlib import Path

from build.sql.mock_data.generate_mock_data import generate_full_mock_data

def main():
    base_path = Path(__file__).parent / "sql" / "mock_data"
    generate_full_mock_data(base_path)

    # TODO: as the DWH schema user loop through the json files in the mock_data directory and load them into the corresponding tables in the DWH. This will ensure that the mock data is available for development and testing purposes.

    load_order = [
        'mq_lookup.json',
        'account.json',
        'employee.json',
        'demographic.json',
        'phone.json',
        'address.json',
        'employment.json'
        ]


if __name__ == "__main__":
    main()