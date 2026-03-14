# Salesforce Connector

server/
├── core/
│   └── http.py                      # (New) Agnostic HTTP wrapper
│
└── connectors/
    └── salesforce/
        ├── __init__.py
        ├── models.py                # Enums, TypedDicts, and NamedTuples
        ├── auth.py                  # OAuth token generation
        ├── client.py                # The Facade (replaces SfSession)
        │
        ├── utils/
        │   ├── __init__.py
        │   └── csv_helpers.py       # All your CSV chunking logic
        │
        └── services/
            ├── __init__.py
            ├── rest.py              # REST API logic
            └── bulk.py              # Bulk 2.0 API logic