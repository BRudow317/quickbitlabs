Q:/quickbitlabs/server/
├── api/
│   ├── auth.py
│   ├── leads.py
│   └── users.py
├── auth/
├── configs/
│   ├── __init__.py
│   └── settings.py
├── core/
│   ├── jwt.py
│   └── security.py
├── engine/
├── models/
│   ├── lead.py
│   └── user.py
├── plugins/
│   ├── lambdalith/
│   ├── oracle/
│   │   ├── __init__.py
│   │   ├── Oracle.py
│   │   ├── OracleClient.py
│   │   ├── OracleDialect.py
│   │   ├── OracleEngine.py
│   │   ├── OracleServices.py
│   │   ├── OracleTools.py
│   │   └── OracleTypeMap.py
│   ├── postgres/
│   │   ├── postgres_utils/
│   │   │   └── type_converter.py
│   │   ├── services/
│   │   │   ├── query.py
│   │   │   └── table_ops.py
│   │   ├── __init__.py
│   │   └── Postgres.py
│   ├── readers/
│   │   ├── reader_utils/
│   │   │   ├── csv_utils.py
│   │   │   ├── filter_null_bytes.py
│   │   │   └── list_from_generator.py
│   │   ├── base.py
│   │   ├── Csv.py
│   ├── scratch/
│   ├── sf/
│   │   ├── services/
│   │   │   ├── __init__.py
│   │   │   ├── bulk2.py
│   │   │   └── rest.py
│   │   ├── tests/
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   ├── csv_utils.py
│   │   │   ├── date_to_iso8601.py
│   │   │   ├── filter_null_bytes.py
│   │   │   ├── list_from_generator.py
│   │   │   ├── soql_utils.py
│   │   │   └── type_converter.py
│   │   ├── __init__.py
│   │   ├── auth.py
│   │   ├── HttpClient.py
│   │   ├── models.py
│   │   ├── repl.py
│   │   └── SalesforceConnector.py
│   ├── utils/
│   │   ├── date_to_iso8601.py
│   │   ├── filter_null_bytes.py
│   │   ├── install_package.py
│   │   └── list_from_generator.py
│   ├── PluginModels.py
│   ├── PluginProtocol.py
│   ├── PluginRegistry.py
│   └── PluginResponse.py
├── services/
│   ├── __init__.py
│   ├── get_customers_request.py
│   ├── MigrationService.py
│   └── plan.md
├── tests/
├── utils/
│   ├── __init__.py
│   ├── encrypt.py
│   ├── helpers.py
│   └── logger.py
├── __init__.py
├── README.md
├── RULES.md
└── start_server.py
